# CAS GC: prove zero owners before a body delete {#cas-gc-zero-proof}

Status: proposal, not implemented. Model-checked in small bounds ([Open points](#open)); reviewed twice against the
code.

## Decision {#decision}

GC sends the conditional body delete of a `pending(t)` row only when it has proved that blob `B` had no owner at the
instant of the round's catalog read. A zero in-degree after the fold is not that proof. The proof adds one condition:
the round saw no addition of an edge for `B` in any ref-log interval it read.

The change has four code parts and one requirement to verify:

1. The addition veto ([Rule](#rule)).
2. A validation of the parent state before any destructive step of the round ([Intake](#intake)).
3. A refusal in the state rebuild ([Rebuild](#rebuild)).
4. A confirmation of the source in two copy paths ([Copies](#copies)).
5. Requirement: the walk covers every namespace life that can hold an edge at the catalog read ([Intake](#intake)).
   The code is believed to meet it; the implementation verifies it and adds tests.

No on-store format changes. No request is added per blob. Added requests: one `HEAD` of `gc/state` per round whose
adopted state holds a `pending` or `standing` row; a decision `HEAD` per vetoed row per round; whatever the source confirmation of a copy needs
([Copies](#copies)).

## Defect {#defect}

A round builds its reference cut per namespace. It can include a later event of one namespace and miss an earlier
event of another. Three schedules remove a body that a committed part still names. Each is reproduced by a unit test
on the real fold (`tmp/intents/reports/crossns-repro.patch`, `tmp/intents/reports/coverage-audit-tests.patch`).

| Id | Schedule |
|---|---|
| D1 | A writer reuses body `t` in the round that condemns it. In the next round's intake a no-bytes copy of that reference lands in namespace 2 and the source part is dropped. The round reads namespace 2 before the copy and namespace 1 after the drop. |
| D2 | As D1, and the copy lands in a namespace created after the round's catalog read. That namespace is not walked. |
| D3 | A writer meets `I(B,t)` after the cut and uploads `u`. The decision `HEAD` sees `u` and commits `pending(u)` while the writer's edge is outside the cut. A copy and a drop in the next round remove `u`. |

`MOVE PARTITION TO TABLE` and a `RENAME TABLE` that changes the namespace copy and drop in one statement.
The marker protocol needs three consecutive zero results and is spared by one hand-over. Two hand-overs remove the
body there too (model `zp2_o4`, `zp3_o4`); two namespaces suffice when pass 2 flips their read order. This is a model
result; no test was built on the marker code.

Two related defects, found by reading the code and then reproduced by unit tests:

| Id | Defect |
|---|---|
| D4 | A deposed leader reads a false end of a ref log. It takes `expected = p + 1` from `ref_fold_memo` with no read and collects its listing witnesses once, before pass 1. While it is paused, a newer leader folds, commits and removes log records above `p` (`cleanupRefObjects`). The stale leader then reads `p + 1` absent and treats the life as proven. With an honest listing this needs the writer's record to be the last listed record of its life and the key the stale leader expects next; a later listed record already holds the life (`witnessAbove`). |
| D5 | `Gc::rebuildBaseline` loses the edges of a live precommit whose manifest is unreadable ([Rebuild](#rebuild)). |

## Terms {#terms}

- `P`: the adopted parent state of this attempt: seal, runs, per-life frontiers.
- `C`: the instant of the catalog `GET` in `Gc::listRefPrefix`. It must follow the lease on `P`.
- Life: one physical incarnation of a namespace. Two lives with the same name are different lives.
- `p[i]`: the frontier of life `i` in `P`. For a life with no parent row: the position before its first record.
- `f[i]`: the last record this attempt consumed from life `i`. A successful end probe, sent after `C`, shows that
  `f[i]` covers every record durable at `C`.
- `saw_add[B]`: true if any fully applied transaction in any `(p[i], f[i]]` emits an activation (`+1`) for `B`.
  It is an OR over single blob deltas. A later removal does not clear it.

## Rule {#rule}

Send `DELETE B If-Match t` if and only if all hold:

1. `P` holds a confirmed `pending(t)` for `B`, and `B` has zero source edges in `P`. The reducer checks this: a
   prior edge of `B` in `P` counts as a veto.
2. The intake is complete: every life admitted at `C` is walked from `p[i]` to a proven end, with no suppressing
   anomaly or hold, and the history and baseline requirements below are met.
3. The fold has zero source edges for `B`, and `saw_add[B]` is false.
4. The delete budget admits the request, and a decision `HEAD` sent after the intake shows the canonical identity `t`.

| Situation | Action |
|---|---|
| Zero, `pending(t)`, `HEAD` shows `t`, `saw_add[B]` | Carry `pending(t)`. `I(B,t)` stays. Commit the fold and the frontiers. No body delete. The next round decides again on its own proof. |
| In-degree above zero | The spare rule applies as it does without this change: the row is dropped. |
| Zero, `HEAD` shows another identity `u` | The transition, unchanged: condemn `u`, remove `I(B,t)`. `pending(u)` needs its own later proof. |
| `pending(t)`, `HEAD` shows no body | `standing(t)`, unchanged. No delete. |
| Fresh condemnation, candidate retry, orphan adoption | Unchanged. Creating an intent needs no proof. |
| `standing(t)` cleanup | Unchanged. The intent removal does not use `saw_add`. |
| Incomplete intake | No destructive step, unchanged. |

The veto is per blob. One vetoed blob does not suppress the round.

## Intake requirements {#intake}

- **Catalog.** One exact `CasRefCatalog::read` per attempt, after the lease. The walk covers every `Live` and
  `Removing` life at `C`, including lives the stream listing omits. Lives are matched by physical incarnation.
  A `Creating` life holds no edge. A life created after `C` is not walked in this round, and need not be: an
  edge in it comes from a writer that arrived after `C` or from a copy of a source that is covered
  ([Why it is safe](#safety)).
- **First sight.** A life with no parent row is replayed from its first record. If the first records are no longer
  available, the attempt takes no destructive step.
- **History.** The round consumes the real successor chain of each life. A cached absence of a successor does not
  prove an end. A jump over records to a snapshot does not establish `saw_add == false`: with an unavailable
  interval the attempt takes no destructive step.
- **Deferred manifest.** A precommit whose manifest is unreadable holds the fold below that transaction and
  suppresses destruction. When the manifest appears, the original activation is replayed and sets `saw_add`.
- **Slice, budget, abort.** A remaining expected key, an omitted probe, a hold or a discarded transaction proves
  no end.
- **Parent validation.** After the last intake read and before the reduce the round sends one uncached `HEAD` of
  `gc/state` and requires the exact version it leased. It is sent when the adopted state holds a `pending` or a
  `standing` row and the round is not already suppressed; other rounds send nothing. Any other answer (another
  version, absent, no answer) suppresses every destructive step of the round that depends on the cut: the body
  delete and the intent deletes of `standing` and spared rows. The reason is reported as `parent_unconfirmed`. The
  round is not aborted: a deposed leader's conditional commit fails anyway. The transition's intent delete of a
  `candidate` row is not behind this read and needs none: it follows a `HEAD` that shows another incarnation.
  Manifest deletes, ref-object cleanup and the sweeps run after the commit. This closes D4.

## Rebuild {#rebuild}

`Gc::rebuildBaseline` must install edges and cursors from committed and precommitted owners. Today a live precommit
with an unreadable manifest gets a hold at `cursor + 1`, and an unrelated append can clear that hold while the
blob edges stay missing (`Gc/CasGc.cpp:5053`). The baseline then undercounts. Status: confirmed by two readers of
the code, not reproduced by a test.

Change: refuse the rebuild in that case. No exemption for a build that looks dead: `buildPrefixEligibleUnderFloor`
reads the current mount floor, not the floor at the recovered cursor, so a build below the floor may have promoted
before it retired, and a `Promote` emits no activation to replay. The operator retries after the stale-precommit
sweep removed the precommit or the manifest became readable.

A pool rebuilt by an older build cannot be recognised from `gc/state` once the hold is cleared. The `GcRebuild`
event's `clamped_shards` or a comparison of current owners with the stored edges shows it. The missing activation
lies below `p[i]`, so neither the parent count nor `saw_add` reveals the owner: the rule above does not protect such
a pool.

A refused rebuild gives back the GC lease it claimed: one conditional write restores the lease fields to their
value before the claim, so a later attempt, which takes a new GC identity, can start. This holds for every refusal
after the claim.

Repair of an undercounted baseline is not provided by this change. A rebuild does not start while another GC
leader's lease is held, and that includes the lease a stopped leader left behind. `cas-migrate-pool` keeps the lease
as it reads it, so a migrated pool whose GC ever ran is in the same position. This is an [open point](#open).

## Rollout {#rollout}

The proof needs every writer and every possible GC leader of the pool to follow these rules. An older writer makes
copies without the source confirmation; an older leader deletes without the veto.

- **Generation 4 pools.** The fix ships in the first build that reads generation 4. Builds of generations 1 to 3
  refuse a generation 4 pool, so no build without the fix can write to it or lead its GC. This holds only if no
  generation 4 build without the fix is ever released.
- **A line that already runs its protocol in the field** (the marker protocol): destructive rounds, including a
  manual `SYSTEM CAS GC RUN`, stay disabled until every server of the pool runs a fixed build and copies started by
  older servers have finished. The code has no pool-wide version barrier for that; it is an operator procedure
  unless a later change adds one.
- **Requests already sent.** A conditional body delete that an older leader sent before the upgrade can still take
  effect afterwards; no rule of a newer leader vetoes it. For a pool that migrates to generation 4 this is the
  residual the migration page already names (a delete of an older build in flight after the stop and the grace).
  This change does not shrink it.

## Copies without reading the body {#copies}

The safety argument needs every such copy to have a source that is live until the destination edge is durable.
What provides it today:

| Path | Source kept live by | Checked after the destination edge is durable |
|---|---|---|
| Relink fetch | `confirmExactRef` after the precommit | yes |
| Repoint | `replaceIfCurrent` inside the promote append | yes |
| `createHardLink`: `ATTACH` / `REPLACE` / `MOVE PARTITION`, `CLONE AS`, local-clone fetch, detach clone, `FREEZE`, mutations | the `DataPartPtr` of the source and `isSharedPtrUnique` in `grabOldParts` | no |
| `republishRef` (`RENAME TABLE` across namespaces) | the exclusive table lock | no |

Two sequences pass today (`tmp/intents/reports/source-liveness-tests.patch`): a forced detach
(`forcefullyMovePartToDetachedAndRemoveFromMemory`) followed by `DROP DETACHED PART` inside a clone or a mutation;
and `republishRef` accepting `Removal::Gone` for a source that another writer dropped before the destination
precommit. The reproducing tests of this document live in patches outside the tree; they are committed as tests
with the fix.

Change: both paths confirm the source after the destination precommit and before the promote, as relink does.

- **What the confirmation must establish.** That the source edge is durable and not being removed, and that this
  server still owns the mount slot. A match in the committed map alone is not enough: a source drop that reached
  the store but is not yet applied to the map still looks live. `Pool::confirmExactRef` checks in-flight mutations
  and slot ownership for this reason; the two paths use the same confirmation. An unknown result refuses the
  promote. If the confirmation needs a request in some state, the path sends it: "no request per blob" stays true,
  "no request at all" is not promised.
- **`createHardLink`.** Each adopted entry carries its original committed source (namespace, ref, manifest ref)
  through staged copies and renames inside the transaction. A copy of an entry staged earlier in the same
  transaction confirms that original source, not the staging path. An entry whose bytes the destination uploads
  itself needs no source. When the source changed, the build is abandoned with a retryable error.
- **`republishRef`.** Three cases:
  - The destination is already committed with equal content (an identical move finished first): success, with no
    new source edge and no confirmation. As today
    (`CASPartFolderAccess.RepublishRefCompletesWhenIdenticalRenameTakesDestinationDuringPublish`).
  - This call precommits a new destination and the source is missing or unconfirmed before the promote: the call
    fails and abandons its precommit.
  - The source disappears after this call's confirmation and promote, removed by an identical concurrent move:
    success, as today (`CASPartFolderAccess.RepublishRefCompletesWhenSourceAlreadyTakenByIdenticalMove`).

## Why it is safe {#safety}

Assumptions:

- Exact-key reads are linearizable. A token never recurs at a key.
- A writer's edge is durable before its first `HEAD`, and the writer can commit only while it owns that edge.
- A no-bytes copy is valid only with a live source that overlaps its durable destination edge
  ([Copies](#copies) makes every path confirm it).
- A confirmed intent precedes `pending(t)`.
- `P` counts every owner at its frontiers ([Rebuild](#rebuild) removes the known way to violate this).
- A `Creating` life holds no edge.

1. Suppose `B` has an owner at `C`. Its life is in the catalog read. If the owner existed at `p[i]`, the parent
   in-degree is above zero. Otherwise its activation lies in `(p[i], f[i]]` and sets `saw_add[B]`, also when the
   owner was dropped before the round read that life. Both contradict the rule. So `B` has no owner at `C`.
2. `I(B,t)` is confirmed before `C`. A chain of no-bytes copies needs an owner at `C` or a first source acquired
   after `C`. There is no owner at `C`. A writer that arrives after `C` meets the intent while `t` is current and
   uploads a fresh incarnation, which `If-Match t` cannot remove.
3. The argument uses no clock and no bound on how late the delete takes effect.
4. A writer `W` that saw `t` before the delete and reads the intent afterwards. `W`'s edge is durable before its
   `HEAD`. If `I(B,t)` is still there, `W` uploads a fresh incarnation. If it is gone, an existing rule removed it
   after a `HEAD` that showed another incarnation `u` (the transition or the spare), or after a later complete zero
   cut over `standing(t)`. The zero cut cannot hold while `W`'s edge is live. In the other case `W` adopts the blob
   whose body is `u`; `u` landed after `W`'s edge, so any round that could prove no owner for `u` sees `W`'s edge
   in the parent count or as an addition. This change adds no intent removal.

D2: the copy's destination is not walked, but the copy needs a source that owned `B` at `C`; step 1 finds that
source. D3: `pending(u)` is committed; the writer's activation is in the next attempt's interval and vetoes.

## Cost and liveness {#cost}

- One boolean per current blob in `foldDeltasIntoGeneration`, set on each `BlobDelta` with `remove == false`.
- One `HEAD` of `gc/state` per round whose adopted state holds a `pending` or `standing` row.
- A vetoed row gets its decision `HEAD` again in each round until it is deleted, spared or moved.
- A blob whose edges are added and removed in every round is not reclaimed while that lasts. After the last drop
  one round carries the row; the next round that is complete, not suppressed and has delete budget deletes.
- A blob that nothing touches is reclaimed as today.

## Open points {#open}

- The marker-protocol line (`antalya-26.6`): the same guard at its body-delete step. Not designed in detail.
- Model coverage. Green, complete runs: three namespaces with relays, two namespaces, two blobs, suppressed
  rounds, and the single-cut model with a deposed leader and late requests. Not modelled: a deposed leader together
  with per-namespace cuts (D4 is closed by argument and by the parent validation); a life created inside a round
  (argued from the code: a first fold starts at the first record). Bounds are small.
- Repair of a baseline that an older rebuild may have undercounted. Today the rebuild does not start on a pool
  whose GC lease is held. Options: the rebuild takes over a held lease by the observation rule the GC loop uses;
  the offline migration tool resets the lease; an audit of owners against stored edges shows whether a pool is
  affected.
- Whether the catalog read follows the lease in the code today, and whether the walk reaches lives that the
  stream listing omits.
- A kept (not dropped) spared row is safe under this rule in the model without an extra rule. It is not part of
  this change.
- `loadOutdatedDataParts` removes duplicate parts without a table lock: whether it can overlap a `RENAME TABLE` is
  not established.
- The trace from a ref-log record to the reducer has no stage that combines or drops a `+1` (checked by reading);
  the exhaustive test of `decideIntentStep` gets a new input and its cell counts must be derived again.

## Tests {#tests}

- D1, D2, D3 in both walk orders: the body stays and no body delete is sent. The row is carried only when the cut
  shows zero with an observed addition; when the walk sees the source before its drop or the destination after the
  copy, the cut is positive and the row is dropped. A quiet round keeps the body while the destination part exists.
  After the destination part is dropped the body is reclaimed.
- A relay over three namespaces, and a relay over two where the second copy goes back into the first. An addition
  and a removal of one blob in one intake. An unrelated blob in the same shard is still reclaimed in that round.
- Continuous add and drop of one content: no body delete while it lasts. After the last drop the body is deleted
  by the second round that is complete, not suppressed, sees no record of the blob and has delete budget for it;
  the test sets those preconditions.
- Lives created before and after each catalog read; removal and re-creation under the same name.
- A snapshot that crosses an unread add and drop; a missing successor; concurrent cleanup; a rebuild with a missing
  precommit manifest followed by an unrelated append.
- A leader deposed before and after the parent validation; a late delete; a lost answer. A paused leader whose log
  records were removed by a newer leader (D4), for the body delete and for the intent delete of a `standing` row.
- A hardlink build whose source is dropped after the destination precommit: the build is abandoned. `republishRef`
  with a source dropped by another writer: the call does not report success.

## Coverage of events inside a round {#coverage}

The defects above passed every existing test because no test places an event between two steps of a GC round, and
none combines a copy without reading the body, a drop of its source and a row that is already `pending`. This change
closes that gap as part of the same work.

- **Facility.** A fold test hook that runs a callback at a chosen point of a round: after the catalog read, between
  the reads of two named namespaces (both walk orders, pass 1 and pass 2), after the intake and before the reduce,
  after the request batch and before the `gc/state` commit. Test code only; the product gains no seam beyond the
  store-answer hooks the fakes already have.
- **Matrix, unit level.** {copy path: `createHardLink` across namespaces, `republishRef`, relink} × {source dropped:
  before the round, inside the intake, after the round} × {row of the blob: none, `candidate`, `pending`,
  `standing`} × {one hop, a relay of two hops, a destination created inside the round}. Each cell asserts: the body
  is present while any committed part names it, no body delete was sent for it, `cas-fsck` reports no dangling
  reference. A parameterized suite; cells that cannot occur are listed with the reason, not silently omitted.
- **Other events inside a round**, one test each: a namespace removed and re-created; a ref-log snapshot that
  covers an unread add and drop; an intake slice cut by the time budget so that one namespace is read over two
  rounds; a leader change between the intake and the commit; a state rebuild between two rounds of a relay.
- **Statements, integration level.** `MOVE PARTITION TO TABLE`, `ATTACH PARTITION FROM`, `REPLACE PARTITION`,
  `CREATE TABLE ... CLONE AS`, a `RENAME TABLE` that changes the namespace, and a fetch by relink, each on content
  that was dropped and re-inserted shortly before, with GC rounds running continuously; the oracle is a full read
  with a checksum and `cas-fsck`. No test of `CLONE AS` or of a cross-namespace `RENAME` exists today.
- **Soak and scenario.** The overlap soak gets operation endings that copy between tables (`ATTACH PARTITION FROM`,
  `MOVE PARTITION TO TABLE`, `CLONE AS` followed by a drop of the source); one scenario card does the same at the
  suite's scales.
- **Model.** The model of record gains a second namespace with its own frontier; the per-namespace model used for
  this fix is kept with its configurations and results next to the other models.
