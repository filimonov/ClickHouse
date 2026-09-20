# State-space budget for the `Base` scenario {#state-space}

`Base` runs two sessions (`Sessions = {k1, k2}`) over two parts. It ran at `TID_MAX = 2` while the measurements
below were taken and now runs at `TID_MAX = 3`, the transaction count the scenario matrix asks for; the
"Where `Base` stands" section records both. As first written it did not
finish: TLC was killed after 20 minutes with 628,420,984 states generated, 115,663,927 distinct and 17.6M still
on the queue, while the one-session `BaseSmall` finished in about a second at 24,667 distinct states. This file
records where the states come from, which model corrections and which fingerprint reduction closed that gap, and
what the two scenarios cost today.

All measurements below were taken on the branch `tla/mergetree-transactions` with `tla2tools.jar` from the
TLA+ release used by `run_tlc.sh`, Java 21, `-workers auto` on a 32-core machine, one run at a time unless the
row says otherwise. Variant copies lived under `tmp/tla/measure/` and are not committed.

## Where the states come from {#where-from}

One session explores a graph of depth 75 whose average outdegree is 1: a transaction is a long, almost linear
chain of fine-grained steps, because every metadata store is three actions, every commit and every rollback walks
its part list one part per action, and a `SELECT` checks visibility one part at a time. Two sessions do not add a
second dimension of the same size, they interleave two such chains, and that is where the states are.

Measuring what a second session adds, by disabling one group of actions at a time, put the whole cost in one
place. Each row is a two-session run with `SYMMETRY`, killed at three minutes; the last row finished instead.

| Variant | Distinct states |
|---|---|
| all `Base` actions | killed at 3 min, 16,243,310, queue 4.1M and growing |
| without `Drop*` | killed at 3 min, 14,213,437 |
| without `Select*` | killed at 3 min, 15,945,251 |
| without the updater | killed at 3 min, 16,076,281 |
| without `SYMMETRY` | killed at 3 min, 18,826,133 |
| **without `KillTransaction`** | **finished: 905,497 distinct** |

Dropping, selecting and the updating thread are each worth at most a few per cent. `KillTransaction` is worth
more than two orders of magnitude, because it is the only action that forces a transaction into rollback while
its owner is in the middle of a statement, so the rollback machine interleaves with the owner's unwind and with
the other session's work.

`KillTransaction` may not be removed: the scenario matrix lists it in `Base`, and the race it creates is one of
the things the model exists to check. Three parts of how it was modelled turned out not to match the C++, and
correcting them is what made the scenario finish.

## Model corrections {#model-corrections}

Baseline C++ is upstream master `2c24b6b9291e`, in this worktree. `KILL TRANSACTION` is executed by
`InterpreterKillQueryQuery::execute` (`src/Interpreters/InterpreterKillQueryQuery.cpp`, the
`ASTKillQueryQuery::Type::Transaction` branch), which calls `MergeTreeTransaction::onException`
(`src/Interpreters/MergeTreeTransaction.cpp`), which calls `TransactionLog::rollbackTransaction`
(`src/Interpreters/TransactionLog.cpp`), which calls `MergeTreeTransaction::rollback`.

**A. The killer must be between its own statements.** `KillTransaction` had no guard on the killing session's
program counter, so a session could issue `KILL TRANSACTION` while it was itself in the middle of an insert, a
select or a drop. A session executes one query at a time, so the action now requires `client[k].pc = "Idle"`.

**B. The `KILL` query returns only when the rollback it started has finished.** `onException` runs
`rollbackTransaction` synchronously on the killer's own thread, so the killer cannot start another statement
while the rollback runs. `KillTransaction` now parks the killer at the new program counter `KillWait`, and the
new action `KillReturn` releases it once no transaction names it as its rollback driver.

**C. A rollback body has exactly one driver.** `MergeTreeTransaction::rollback` opens with a
`compare_exchange_strong` on `csn` and returns `false` to every caller that loses it, so the body that marks the
created parts, outdates them, restores the removed ones and unlocks their removal `TID` runs on exactly one
thread. The model added the killer to `holders` while leaving the owner there, and `Drives(k, t)` let any holder
take the next rollback step, so the owner and the killer could alternate arbitrarily across the whole body. The
kill now names the winner of the compare-and-exchange in `txn[t].rb_driver` and leaves `holders` alone, which is
what the code does: a `KILL` destroys no `MergeTreeTransactionPtr`, so it acquires no holder. `Drives(k, t)`
reads `rb_driver`.

None of the three removes a race between the killer and the owner: the kill can still land at any point of the
owner's statement, and the owner still unwinds concurrently with the rollback. What they remove is a killer that
runs two queries at once and a rollback body executed by two threads at once, neither of which the server does.

Measured one at a time, two sessions, `SYMMETRY`, no fingerprint reduction:

| Configuration | Distinct states | Time |
|---|---|---|
| none of the corrections | killed at 5 min, 30,950,018; killed at 20 min, 115,663,927, queue growing |  |
| A only | killed at 5 min, 30,935,687 |  |
| A + B | killed at 5 min, 34,519,014 |  |
| C only | 9,276,135 | 1 min 19 s |
| A + C | 6,220,768 | 51 s |
| A + B + C | 2,247,186 | 19 s |

C is the correction that makes the scenario finite in practice. A and B are worth nothing on their own, because
constraining the killer does not constrain the alternation between the two drivers, and a further factor of 2.8
once C is in place.

## Reductions {#reductions}

**`VIEW BaseView` in `MC_Base.tla`.** The fingerprint drops three kinds of field. Fields that no `Base` action
and no `Base` property reads: `h.content`, `h.snapshot`, `txn.csn_notified`, `client.outcome`,
`client.outcome_tid`, `part.pins`, and `client.last_error` beyond the single value `NoSpuriousStaleVersion`
tests. Fields that are a function of the ones kept: the durable disk layer, which `DISK_MODE = "Durable"` keeps
equal to the cached layer, and a read's `frags`, which an empty `Covers` and a constant payload make a function
of the read's `parts`. Fields no `Base` action writes: `mdisk`, `mut`, `task`, the mutation and non-transactional
parts of `h`, the tail-pointer and unknown-state parts of `tlog`, the fault counters and loading fields of `sys`,
and a frame's `noexcept_retries`. All three arguments depend on the constants in `MC_Base.cfg`, so another
scenario needs its own view rather than this one.

| Configuration | Distinct states | Time |
|---|---|---|
| A + B + C, no view | 2,247,186 | 19 s |
| A + B + C, with `VIEW BaseView` | 1,814,598 | 16 s |

Worth 19%. Two things are worth noting about what the view does not buy. Measured before the corrections, on a
run that does not finish, a projection of the same shape reached 28,045,227 distinct states in five minutes where
the unprojected run reached 30,950,018, both still at breadth-first level 43 with a growing queue: a difference of
that size would not have made the scenario finish, whatever the remaining total was. And `part.pins`, which looks like the largest ghost in the state, is worth
nothing at all: a pin is added and removed exactly where a transaction list, a program counter or a rollback
phase already changes, so it is redundant with fields the view keeps rather than independent of them.

**`SYMMETRY SymSessions`**, kept. With the corrections in place it is worth a factor of 2.

| Configuration | Distinct states | Time |
|---|---|---|
| A + B + C, no `SYMMETRY` | 4,494,312 | 34 s |
| A + B + C, `SYMMETRY SymSessions` | 2,247,186 | 19 s |

Session symmetry cannot be worth much more than that here, and parts cannot be added to it. `Begin` hands out
transaction identifiers from a monotone counter, so two sessions stop being interchangeable as soon as the first
one begins; and `SetToSeq`, which turns the set of parts a drop found into the sequence it walks, is a `CHOOSE`,
so the next-state relation is not equivariant under a permutation of `Parts` and TLC's symmetry reduction would
be unsound over them.

**Removing `Fsync` from the store steps of `BaseNext`**, measured and not applied. `Fsync` requires
`DISK_MODE = "Layered"` and `Base` is `Durable`, so the action is already disabled by its own guard; spelling a
store next-state relation without it changed neither the state count (2,247,186 either way) nor the time. The
shared module keeps one `StoreNext`.

## Bounds {#bounds}

No state constraint is applied. The two that the plan held in reserve were measured and are not needed: bounding
the concurrently open store frames to 2 left a two-session run at 8,678,732 distinct states after three minutes
and still growing, and `TID_MAX` already bounds the transactions that can reach `CommitAck` or
`RollbackFinalize`, because `Begin` draws from a monotone counter capped at it, which makes that constraint
vacuous at any value of the bound.

A third candidate, outside the plan's list, was built and measured too: restricting the kill to a session that
holds no transaction of its own. It reached 23,908,162 distinct states after three minutes and was still growing,
and it would also have cost the case where the kill lands while two transactions are live, so it was discarded on
both counts.

`CSN_MAX` is a bound and is set to the smallest one that works. Real commit sequence numbers start at
`FirstCSN = 33`, the log starts with one entry there, and `KeeperCanAppend` requires `zk.seq < CSN_MAX`, so each
unit above 33 buys one commit. At `TID_MAX = 2` the value was 35, for two commits; at `TID_MAX = 3` it is 36,
for three. Lowering it would disable `CommitCreateCSN` for the last commit, and raising it buys nothing, because
`TID_MAX` already caps the transactions that can reach `CommitCreateCSN`. That last point was measured rather
than assumed: the same modules at `CSN_MAX = 38`, which allows five commits, reach 28,553,697 distinct states
against 28,553,007 at 36, a difference of 0.002%, entirely inside the multi-worker counting noise.

## Where `Base` stands {#final}

Both scenarios green, `-workers auto` on 32 cores, one run at a time, `-Xmx16g` as `run_tlc.sh` sets it. Every
count here is approximate: under `-workers auto` the totals move by a few states between runs, because two
workers can fingerprint the same state before either has inserted it.

| Scenario | Bounds | States generated | Distinct states | Time |
|---|---|---|---|---|
| `BaseSmall` | `TID_MAX = 2`, `CSN_MAX = 35` | 66,399 | 47,381 | 1 s |
| `Base` | `TID_MAX = 3`, `CSN_MAX = 36` | 67,864,730 | 28,553,114 | 4 min 08 s |
| `Base`, after the final-review fix | `TID_MAX = 3`, `CSN_MAX = 36` | 67,864,300 | 28,552,935 | 4 min 06 s |
| `Base`, superseded | `TID_MAX = 2`, `CSN_MAX = 35` | 5,138,339 | 2,163,747 | 19 s |
| `Base`, after the merge commit | `TID_MAX = 3`, `CSN_MAX = 36` | 67,850,038 | 28,547,508 | 4 min 12 s |

The merge commit is the first change to move `Base` by more than the counting noise: 28,547,508 against
28,552,913, which is 5,405 fewer, about 0.02%. Two changes of the same small size pull against each other. The
`attached` component left `stmt`, which is in `BaseView`, so states that differed only in a ghost no action read
now merge; and a holder is now removed by the owner that destroys it, `RollbackReturn` or the detaching branch
of `RollbackStart`, rather than by `RollbackFinalize`, so a rolled-back transaction keeps its holder for a step
or two longer and states that were equal now split. The merge is worth slightly more than the split. `holders`
is no longer a write-only ghost either: every step of the merge task reads it through `Holds(i)`, which is the
shared pointer that keeps the transaction alive.

The final-review fix commit, which made the statement rollback act on every precommitted part and routed
`Refuse` to it whenever the statement transaction is non-empty, left the count where it was: 28,552,935 against
28,553,114 is the multi-worker noise, which the cleanup task later confirmed by re-running an unmodified
`630d8ad28674` and getting 28,553,740. `stmt` is in `BaseView`, so a reachable change there would have shown. It
is not reachable in `Base`: `QUERY_FAULTS_MAX = 0` disables `Fail`, and with an empty `Covers` there is no
`PublishEnrol`, so no refusal can arrive while `stmt.precommitted` is non-empty. Plan 2's covering relation is
what makes the path live, and the count has to be re-measured there.

The third transaction is worth a factor of 13 in states and 13 in time, and takes the complete search depth to
117. It was taken anyway: the scenario matrix names `TID_MAX = 3` for every scenario, and the bound contract
allows a reduction only while every witness of every property the scenario checks stays red. Three witnesses
could not reach their target at `TID_MAX = 2` and needed a separate `BaseWitness` scenario to be shown at all,
which is exactly the contract not being met. `BaseWitness` is gone and its three rows are `Base` rows now.

Raising the bound also produced the model's first counterexample on a baseline run: `Atomicity` went red at
7,982,042 distinct states, on a read a transaction takes while dropping a part another transaction created and
committed under it. Three transactions are the fewest that can build that shape. The trace, the C++ it was
judged against and the property correction are in `FINDINGS.md`, finding F1.

It also settled an open question about a witness. `Assert_validateInfo_removal` had to be built as a two-change
witness at `TID_MAX = 2`, because the one change the design document names left the run green there. At three it
is red on its own, so the witness is now the one-change witness the document always described. `FINDINGS.md`,
section 3, has the trace shape and why three transactions are the fewest that reach it.

The `TID_MAX = 2` figures are above the measurement tables further up, and for one reason: those were taken
while a session could not kill its own transaction. Allowing it, which is what the C++ does, takes `Base` from
1,814,603 to 2,163,747 on the same modules, and takes `BaseSmall` from 24,667 to 47,381, because with one
session that is now the only way a transaction is ever killed. Everything else the corrections and the view do
is unchanged by it.

## The `SetSnapshot` scenario {#setsnapshot}

`SetSnapshot` is `Base` plus three things: the action `SetSnapshot`, the updater's truncation pass
(`UpdRemoveOldEntriesSetTail`, `UpdRemoveOldEntriesDelete`, `UpdRemoveOldEntriesDone`), and the cleanup group
(`CleanupGrab`, `CleanupValidate`, `CleanupDeleteOk`, `CleanupDeleteFail`). Its bounds are `Base`'s, with two
constants added: `SNAPSHOT_TARGETS = {33}` and `SET_SNAPSHOT_PROTECTS = FALSE`. `MC_SetSnapshotFixed` is the
same scenario with `SET_SNAPSHOT_PROTECTS = TRUE`.

**It does not finish inside the budget, and that is the scenario's open problem.** Four runs at the matrix
bounds were killed; none of them was converging. The state count is not a mystery and not a defect of any one
addition: measured at equal bounds the scenario is 3.4 times `Base`, 7,420,069 distinct against 2,163,747 at
`TID_MAX = 2`, which puts the matrix bounds near 98 million and about 15 minutes at the 6.9 million distinct
states a minute the runs sustain. The reductions below were applied and measured; they moved it by single-digit
per cent, because every addition multiplies and none dominates.

`SNAPSHOT_TARGETS` is one value and stays one value. `FirstCSN` is the oldest real CSN the scenario can name,
and a snapshot at or above the one a transaction already holds cannot make a part prematurely removable, so
the one interesting target is the oldest one. With the change guard on `SetSnapshot` one value also means each
transaction can retarget at most once, which is the "at most once per transaction" bound without a counter.

### Where the growth is {#setsnapshot-ablations}

Three variants under `tmp/tla/measure/`, each killed at three minutes, all with `SetSnapshotView`. The point of
the table is the negative result.

| Variant | Distinct states at 3 min |
|---|---|
| `M0`, `Base` actions only, `SNAPSHOT_TARGETS = {}` | 14,859,870 |
| `M1`, `Base` actions with `SET TRANSACTION SNAPSHOT` live | 15,781,536 |
| `M2`, `Base` actions with the truncation pass live | 14,523,121 |

All three are within 8% of each other. No single addition is the multiplier; each crosses its own dimension
with the whole `Base` space, and the three-minute mark is too early in the breadth-first search to separate
them anyway.

### Reductions applied and what each was worth {#setsnapshot-reductions}

Every row is a two-session run at `TID_MAX = 3`, `CSN_MAX = 36`, compared at the same elapsed time, because
none of them finished. The logs are under `tmp/tla/old_logs/`.

| Configuration | Distinct states | At |
|---|---|---|
| as first written | 57,590,498 | 8 min 13 s; killed at 520 s with the queue growing from 2.97M to 5.91M |
| plus the three changes below | 57,317,500 | 8 min 03 s; killed at 10 min at 70,926,154, queue 4.94M |
| plus `CONSTRAINT AtMostOneSetSnapshot` | 63,177,430 | 9 min 03 s, against 64,111,651 for the row above at the same minute |
| the constraint removed again | 56,968,754 | 8 min 03 s |

The three changes, together worth nothing measurable:

1. **`SetSnapshot` is guarded on the snapshot actually changing.** `setSnapshot` storing the value the
   transaction already holds changes nothing the server can observe, and the model also restarts the read
   baseline there, so without the guard every idle point of every running transaction produced a state. This
   is a model correction, not a bound.
2. **`h.content` left `SetSnapshotView`.** It is captured history rather than a function of the current state,
   so it splits states that are otherwise equal. No property read it while `NoLostVisibleData` was not checked.
   Both it and `part.pins` are back in the projection now that the cleanup group exists; what that cost is
   measured in the next section.
3. **The truncation pass got the updater's program counter.** `removeOldEntries` runs on one thread and never
   interleaves with itself, so `SetTail` takes `sys.updater_pc` from `"Idle"` to `"Delete"`, each removal is a
   step of the `"Delete"` phase, and `UpdRemoveOldEntriesDone` ends the pass. This is the more faithful shape,
   and it is the reason the three changes net out to zero: the counter removes a second pass starting inside
   the first, and pays for it with a new state field and a new action.

`AtMostOneSetSnapshot` was measured at 1.5% and **not applied**. With a single target only a transaction that
began above it can move at all, so two transactions holding moved snapshots is already close to unreachable and
the constraint has almost nothing to prune. A bound that buys 1.5% is not worth the sentence it costs.

### What the cleanup group cost {#setsnapshot-cleanup-cost}

Adding the four cleanup actions and putting `part.pins` and `h.content` back into `SetSnapshotView` roughly
doubles the scenario at its exhaustive bounds. Both fields had to come back: `CleanupGrab` reads `part[p].pins`
in its `isSharedPtrUnique` guard, so two states differing only in a pin no longer have the same successors, and
`NoLostVisibleData` reads `h.content`. Leaving either out would make the view unsound rather than merely
coarse.

| Configuration | Distinct states | Time |
|---|---|---|
| `SetSnapshot` before the cleanup group | 7,420,004 | 1 min 05 s |
| `SetSnapshot` with it | 13,634,229 | 2 min 01 s |
| `SetSnapshotFixed` before | 7,291,951 | 1 min 04 s |
| `SetSnapshotFixed` with it | 13,104,416 | 2 min 01 s |

Both stay well inside the 30 million distinct states and 10 minutes a scenario is budgeted, so no further
reduction was sought.

The review round that followed added a parts-lock conjunct to `CleanupDeleteOk` and `CleanupDeleteFail`, because
`removePartsFinally` and `rollbackDeletingParts` both run under `lockParts`. A conjunct can only restrict, so it
cannot add states, and the pair moved from 13,634,354 and 13,104,253 to 13,634,229 and 13,104,416: down 125 in
one and **up 163** in the other. The 163 is not a state the conjunct created. It is the multi-worker counting
noise this file already records elsewhere, in which two workers can fingerprint the same state before either has
inserted it; the band is a few hundred states at this size, and both differences are inside it. The two factors are not separable by these runs: the actions and the two view fields
arrived together, and a run with the actions and the old view would be unsound to compare against.

### The `SetSnapshotF2` pair {#setsnapshotf2}

`MC_SetSnapshotF2` and `MC_SetSnapshotF2Fixed` are one session, one part, `TID_MAX = 3`, `CSN_MAX = 35`,
`SNAPSHOT_TARGETS = {34}`, with the same view and the same action set. They exist because the violating
behaviour of finding F2 is a single sequential run of three transactions, and breadth is the wrong resource for
reaching it. A probe at the witness bounds with two sessions, two parts and only `NoPrematureDelete` checked
was killed at 333 seconds with 131,878,184 states generated, 46,726,144 distinct, at depth 43, and a queue of
8.2 million still growing; the violation is at depth 45. A run past 30 million distinct that is still growing
is a defect of the configuration rather than something to wait for, and the defect here is breadth: two
sessions and two parts cross the whole space with a behaviour that uses neither. Cutting the universe to what
the trace uses puts it at about 170,000 distinct states and three seconds.

| Configuration | Distinct states | Time | Result |
|---|---|---|---|
| `SetSnapshotF2` | 170,881, a first-violation count | 3 s | red on `NoPrematureDelete`, 45 states |
| `SetSnapshotF2Fixed` | 367,183 | 4 s | green |

The `Fixed` run is larger than the red one because it finishes; the red one stops at the first violation, so
its count is not reproducible. Two runs of it here gave 177,195 and 170,881, and a reviewer's gave 167,361, all
on the same 45-state trace. Every first-violation count in these documents is an order of magnitude, not a
figure to match.

### The bounds, and why there are two sets {#setsnapshot-bounds}

The scenario is checked exhaustively at `TID_MAX = 2`, `CSN_MAX = 35`, two sessions, and its witnesses are shown
at the scenario matrix's `TID_MAX = 3`, `CSN_MAX = 36` in `MC_SetSnapshotWitness`. A witness run stops at the
first violation and can afford bounds an exhaustive run cannot, which is the general point recorded as spec
defect S7 in `FINDINGS.md`; plan 1 deleted `MC_BaseWitness` for having this shape, and needing it again one task
later is what settled the question.

| Configuration | Distinct states | Time | Result |
|---|---|---|---|
| `SetSnapshot`, `TID_MAX = 2`, `CSN_MAX = 35`, two sessions | 7,420,004 | 1 min 05 s | green, **committed** |
| `SetSnapshotFixed`, same bounds | 7,291,951 | 1 min 04 s | green, **committed** |
| `SetSnapshot`, `TID_MAX = 3`, `CSN_MAX = 36`, two sessions | 56,968,754 after 8 min, queue 4.47M | killed, five times | does not finish; witness bounds only |
| `SetSnapshot`, `TID_MAX = 3`, `CSN_MAX = 36`, one session | 1,271,599 | 10 s | green, rejected |
| `Base`, for scale, `TID_MAX = 2`, `CSN_MAX = 35` | 2,163,747 | 19 s | the 3.4x factor comes from this pair |

Five witnesses are not red at the exhaustive bounds, which is debt B1 in `FINDINGS.md`. Four of the five are
`Base` witnesses that `Base` already verifies at `TID_MAX = 3`: `SingleRemover`, `NoUncommittedRead`,
`NoLostRead` and `Assert_validateInfo_removal`, all four for the reason plan 1 documented, that the shape needs
a third transaction. The fifth is the sortedness conjunct of `Assert_getOldestSnapshot`, which is the property
this scenario adds and which `Base` cannot verify because `Base` does not enable `SetSnapshot`. That one is what
`MC_SetSnapshotWitness` exists for, and it is red there in four seconds. The property's other two conjuncts have
witnesses of their own and are red at the exhaustive bounds.

The one-session configuration is cheaper still and was rejected outright: a session runs one transaction at a
time, so two transactions are never running together and `Assert_getOldestSnapshot` cannot be falsified at all,
at any `TID_MAX`.

## The `Merge` scenario {#merge}

`Merge` is `Base` plus the background merge task, the cleanup group and the updater's truncation pass, over a
part universe of three: `P1`, `P2` and `M12`, which covers them. It is the first scenario with an actor that is
not a session, and the first with a non-empty covering relation.

### Bounds, and why one session {#merge-bounds}

The scenario matrix's bounds are `TID_MAX = 3`, `CSN_MAX = 36` and two sessions, and at two sessions it does not
finish. Two runs were killed. The first reached 51,147,824 distinct states in 9 minutes at breadth-first level
54, with 5.2 million queued; the second, on the tree this section is committed with, reached 44,277,426 in 8
minutes at level 53, with 4.7 million queued and the queue growing by about 430,000 a minute. A run past 30
million distinct that is still growing is a defect of the configuration rather than something to wait for, which
is the rule `SetSnapshot` established.

The reduction is one session, and it is the second rung of the ladder rather than the first because the first
buys nothing here. `TID_MAX` stays at 3: `MergeBegin` draws from the same counter as `Begin`, so the scenario
trades a client transaction for the merge and runs at two client transactions plus one merge. Raising it to 4
would make the run larger, not smaller. The third candidate, a constraint bounding the scenario to one live
covering part, is already implied by the universe, which has exactly one, and would prune nothing.

| Configuration | Distinct states | Time | Result |
|---|---|---|---|
| `Merge`, two sessions | 44,277,426 after 8 min, queue 4.7M and growing | killed twice | does not finish |
| `Merge`, one session | 5,196,830 | 49 s | green, **committed** |

One session is sound for what this scenario is for, and the merge task is the reason. `Merge`'s own properties,
`NoDoubleRead`, `ActiveSetShape` and `NoPrematureDelete`, are about a reader against a merge and a cleanup
thread against a merge, not about two readers; the second actor those races need is the task, which is there at
any number of sessions. The `KILL TRANSACTION` race that made `Base` expensive is also still there, because a
single session can kill its own transaction and can kill the merge's. What one session does cost is checked
rather than assumed: every witness the scenario runs is in `WITNESSES.md` with its verdict at these bounds, and
the four the scenario matrix names for it are red.

One task, not the matrix's two, for a reason of the universe rather than of the budget. There is exactly one
covering part, so there is exactly one merge; a second task could only contend for the same two sources, which
the reservation excludes, and it would add idle-state permutations and no interleaving. The consequence is that
the reservation clause of `ActiveSetShape` is vacuous here. It is checked in the scenario that runs a merge and
a mutation side by side, and the debt is in `WITNESSES.md`'s deferred table with that scenario as its
destination.

### The witness bounds, at two sessions {#merge-witness-bounds}

What one session costs was measured rather than argued, and three witnesses go green at it, which is debt B3.
`MC_MergeWitness` is the same module at two sessions, used by `witness.sh` only, and it is the second set of
bounds spec defect `S7` allows: a witness run stops at the first violation, so it can afford a configuration no
exhaustive run finishes at. Two of the three fire there, `NoSpuriousStaleVersion` in 14 seconds and
`NoUncommittedRead` in 46; the third, `Assert_validateInfo_removal`, was killed unfired at 34,184,268 distinct
states and is placed in plan 5's budget and calibration task. The view is `MergeView`'s, renamed, because what a
projection depends on is which actions are enabled and which properties are checked, and neither differs from
`MC_Merge`.

### Was the growth a model defect? {#merge-growth-not-a-defect}

Asked before the bound was reduced, because a scenario that does not finish is more often a model defect than a
bound. Three candidates were checked against the actions, and none of them holds.

A task cannot start a second merge while the first is in flight. `MergeBegin` requires
`task[i].kind = "Idle" /\ task[i].pc = "Idle"`, and the only two steps that return a task to `IdleTask` are
`MergeCommitFinalize` and `MergeUnwind`, which are the ends of the two paths.

`reserved` is released, at exactly those two steps, with the rest of the task record. It is deliberately not
released at `MergePublishFlip`, which is spec defect `S10`: `CurrentlyMergingPartsTagger::finalize` runs at the
end of `MergePlainMergeTreeTask::finish`, after the commit.

A merge is not selectable on every step. `MergeSelect` requires `part[r].pstate = "Absent"`, and no action of
any scenario returns a part to `Absent`: `MergeWrite` takes the result to `Temporary` and the furthest it can
fall back is `Outdated` or `Deleted`. So `M12` is written at most once in a behaviour, and there is at most one
merge per run — which is also the C++'s "at most one merge per partition at a time", enforced there by
`CurrentlyMergingPartsTagger` and here by that plus the reservation.

What the reduced run confirms is the same thing from the other side: at one session the scenario terminates
with an empty queue, at a complete search depth of 127. (The run's one `Progress` line is at breadth-first
level 43, three seconds in; TLC prints that line once a minute and the run finishes before the next one, so 43
is where the report stops, not where the search does.) An actor that could restart itself, or a reservation that leaked, would not
terminate at any session count. The growth is the ordinary product of a third part, a task with thirteen program
counters, a fourth actor's worth of commits and the cleanup thread crossing the whole of `Base`.

The exhaustive-versus-witness split of spec defect `S7` is therefore not used here and there is no
`MC_MergeWitness`. That pattern exists for a scenario whose exhaustive run does not finish at bounds its
witnesses need; `Merge` finishes at 49 seconds at the bounds every one of its witnesses runs at, which is the
condition `S7` asks for when it can be met.

### What the view keeps {#merge-view}

`MergeView` is `BaseView` plus every field the three additions read or write, and minus one of `BaseView`'s
three justifications. `task`, `part.payload`, `part.pins`, `h.content`, `h.truncated`, `tlog.tail_ptr`,
`tlog.updated_tail_ptr`, `sys.cleanup_pc` and `sys.cleanup_part` are in, for the reasons the `SetSnapshot`
section already gives for the last six.

The justification that does not survive is `BaseView`'s "a read's frags are a function of its parts". It held
because `Covers` was empty and every payload constant, so every root expanded to itself. With `M12` covering
`P1` and `P2` a read of `{M12}` and a read of `{P1, P2}` have the same fragments and different parts, and
`NoDoubleRead` is stated over fragments alone. `client[k].first_read.frags` and `client[k].last_read.frags` are
therefore in the fingerprint.

### A witness result read from the wrong directory {#witness-directory}

Worth recording because it cost a round. `witness.sh` wrote every run to `tmp/tla/w_<WitnessName>/`, with no
scenario in the path, so a `Merge` sweep overwrote the `Base` log of the same witness name. The
`Assert_validateInfo_removal` row was then read as a `Base` regression — green after the extraction, where it had
been red before — when what was in the directory was the `Merge` run, green there for the reason debt `B3` gives.
Re-run on `Base` after the extraction it is red at 53,619,513 distinct states in 5 min 31 s, against 53,133,545
in 5 min 21 s before, so the extraction moved neither the hook nor the shape. The script now writes to
`tmp/tla/w_<Scenario>_<WitnessName>/`.

## The `NonTxn` scenario {#nontxn}

`NonTxn` is `Base` plus the non-transactional queries and the removal batch (`NtInsert*`, `NtDrop*`,
`NtBatch*`) and the cleanup group, over a part universe of three: `P1`, `P2` and `E`, the empty part a
non-transactional `DROP PARTITION` publishes over them. It is the first scenario in which a writer that is not
a transaction changes the active parts set.

No exhaustive run of the whole scenario finishes, so it is checked as two halves, `NonTxnDrop` and
`NonTxnInsert`. Each is a strict sub-scenario of it and keeps two sessions and all three parts; what only the
undivided scenario has is a behaviour that needs both halves at once, which is finding F5's route 2 and is
produced by `MC_NonTxnF5` instead. `NonTxnSpec` itself stays in `MergeTreeTransactions.tla`, because the
modules that produce findings F4 to F6 need both halves in one behaviour.

### Bounds, and why `TID_MAX = 2` {#nontxn-bounds}

The scenario matrix asks for `TID_MAX = 3`, `CSN_MAX = 36` and two sessions, and at those bounds it does not
finish. The run was killed at 27,234,570 distinct states after four minutes, at breadth-first level 43, with
5.34 million queued and the queue growing by about 1.2 million a minute. A run past 30 million distinct that is
still growing is a defect of the configuration rather than something to wait for, which is the rule
`SetSnapshot` established.

The reduction is `TID_MAX = 2`, `CSN_MAX = 35`, and it is the second rung of the ladder because the first rung
is not available here. Cutting to one session would remove the scenario's whole subject: `NtInsertWrite` and
`NtDropWrite` require `~HasTxn(k)`, and a session holds its transaction from `Begin` until it commits or rolls
back, so with one session a non-transactional query and a running transaction can never overlap. Every race the
batch exists for -- a target locked by a transaction, a target created by a transaction that has not committed
-- needs two sessions. `MC_NonTxnF5` is one session, and it reaches its finding only because the
non-transactional work there is sequential rather than concurrent.

The cost of the reduction is the usual one, and it is recorded as a bound-contract debt rather than hidden:
the witnesses that need a third transaction are the four `Base` already verifies at `TID_MAX = 3`.

**And it does not finish there either.** At `TID_MAX = 2` the run was killed at 59,047,617 distinct states
after nine minutes with 7.45 million queued and the queue growing by about 650,000 a minute. The session count
has no further rung: one session removes the overlap between a transaction and a non-transactional query, which
is what every race here is, and the part universe is already the three the covering relation needs. What is
left is the transaction count and the action groups, and those are the two levers the split and the drop-half
budget below use: one transaction with the cleanup group, two without it. A part universe of two with `Covers[E] = {P1}` was considered and rejected without
measuring it: with a single covered part a batch has one target, and `NtBatchRefusedUnchanged`'s subject -- a
refusal after an earlier target was already stamped -- cannot exist, so its witness would go green and the
bound contract would break in the one place this scenario is about.

### The split {#nontxn-split}

What is left is to check less than the whole scenario in one run. `NtNext` is divided into `NtInsertNext` and
`NtDropNext`, and two configurations are committed:

- `NonTxnDrop` is `BaseNext \/ NtDropNext \/ CleanupNext`: the non-transactional `DROP PARTITION` and the whole
  of the removal batch, racing the transactions of two sessions. This is the half the task is about.
- `NonTxnInsert` is `BaseNext \/ NtInsertNext \/ CleanupNext`: a non-transactional `INSERT` racing a
  transaction.

Both are at `TID_MAX = 2`, `CSN_MAX = 34`, two sessions, three parts. `CSN_MAX` comes down by one from the
undivided figure because 34 is the smallest value that allows the two commits `TID_MAX = 2` permits, and every
unit above `FirstCSN = 33` costs states.

`NonTxnInsert` finishes green. `NonTxnDrop` did not, at any configuration tried while the scenario was
written: 32.5 million and 44.1 million distinct at `CSN_MAX = 35`, and 56,703,779 after 9 min 30 s at
`CSN_MAX = 34` with 3.39 million queued and the queue still growing by about 110,000 a minute. That was model
defect `M15` and debt `B4`, and the budget for it belonged to task 5.

### The budget of the drop half {#nontxn-drop-budget}

Task 5 re-measured the committed configuration first, with the `NonTxnDropView` the split had added, and it
still does not finish: killed at 32,818,006 distinct states after five minutes, at breadth-first level 66, with
2.71 million queued and the queue growing by about 260,000 a minute.

The first lever tried was the one the task named: a ghost counter of the non-transactional queries a behaviour
has issued, with a `CONSTRAINT` allowing one. It was measured rather than assumed, and it is **rejected**: at
one query per behaviour the run reached 32,216,335 distinct states in the same five minutes with the queue at
2.70 million and still growing, a cut of about two per cent. A second `DROP PARTITION` needs the first one to
have been refused and unwound before the empty part is `Absent` again, so there are few behaviours with two of
them and bounding them buys nothing. The counter was reverted with the constraint, because a ghost field that
buys nothing is a field a later reader has to account for.

What the space is made of is transactions, not queries. Cutting `TID_MAX` from 2 to 1 takes the half from over
32 million to **1,112,076 distinct states in 11 seconds**, a factor of thirty, and that is the committed
exhaustive configuration of `NonTxnDrop`: two sessions, three parts, `TID_MAX = 1`, `CSN_MAX = 34`. It is a
**bound**, not a reduction that costs nothing, and what it costs is stated as one: with a single transaction in
the behaviour, seven witnesses of the half's own roster go green, and they are the rows the second
configuration below and `MC_NonTxnWitness` exist to pay.

One transaction still keeps the subject. The batch refuses on a target that is locked or whose creator has not
committed, and one transaction can hold either; both batch witnesses, `NtBatchRefusedUnchanged` and
`NtRefusalJustified`, are red at these bounds, as is `Assert_validateInfo_nocreation`, the two-change witness
whose minimality debt `B5` is paid here.

### The second drop configuration, at two transactions {#nontxn-drop-two}

`NonTxnDropTwo` is `BaseNext \/ NtDropNext`, the drop half **without the cleanup group**, at `TID_MAX = 2`,
`CSN_MAX = 34`, two sessions and three parts. It finishes green at **47,958,711 distinct states in 7 min 34 s**.

That is above the 30-million heuristic, and it is accepted anyway, because the heuristic is about runs that do
not converge rather than about a number: the queue of this run peaked at 1.21 million at level 72 and then
drained to zero, which is the opposite of what every killed run above did. The pair is what the half needs:
the cleanup thread and a second transaction each cost more than the budget allows together, and each of the two
configurations pays the witnesses the other one loses. The cleanup properties -- `NoPrematureDelete`,
`PinnedNotDeleted`, `NoFalseCorruption` -- are out of `NonTxnDropTwo`'s roster rather than vacuous in it, and
the four rows that need a second transaction are in it.

Two rows are paid by neither, because they need the cleanup thread **and** a second transaction:
`Assert_validateInfo_order` and `SingleRemover`, and both are red in `MC_NonTxnWitness`, the undivided module at
witness bounds, which is what the two-bound-sets rule of spec defect `S7` is for. `NoDoubleRead` and
`NoUncommittedRead` are red in neither and in no configuration that finishes; they are what is left of `B4`,
with the counts they reached, and they are placed in plan 5's budget and calibration task.

### What the view keeps {#nontxn-view}

There is no `NonTxnView`: the undivided scenario has no exhaustive configuration, and each committed module
carries its own projection. `NonTxnDropView`, `NonTxnDropTwoView` and `NonTxnInsertView` are the same shape and
the paragraph below is their common argument; `MC_NonTxnF2` has a fourth, `NonTxnF2View`, which is
`SetSnapshotView` verbatim because that module has no batch and an empty covering relation.

The shape is `MergeView`'s, because `Covers` is non-empty here too and a read's fragments are therefore not a
function of its parts. Three changes. `task` is dropped, because `Tasks = {}`. `tlog.tail_ptr`,
`tlog.updated_tail_ptr` and `h.truncated` are dropped, because the matrix does not give `NonTxn` the truncation
pass and no action writes them. `sys.nt_batch`, `h.batch` and `h.batch_outcome` are added, because the batch
actions read all three and the two batch properties read the last two. `h.abandoned` is in every view in the
tree, `BaseView` included, from this task on: the visibility oracle reads it. `NonTxnDropTwoView` keeps
`sys.cleanup_pc` and `sys.cleanup_part` although its cleanup group is off, so that the two drop modules differ
in their constants and their `Next` and not in their projection; a field no enabled action writes is constant
and costs nothing to keep.

### The runs {#nontxn-runs}

| Configuration | Result | Distinct states | Time |
|---|---|---|---|
| `NonTxn`, `TID_MAX = 3`, `CSN_MAX = 36`, two sessions | killed, still growing | 27,234,570 after 4 min, queue 5.34M | |
| `NonTxn`, `TID_MAX = 2`, `CSN_MAX = 35`, two sessions | killed, still growing | 59,047,617 after 9 min, queue 7.45M | |
| `NonTxnDrop`, `CSN_MAX = 35` | killed, still growing | 32,491,000 and 44,148,000 on two runs | |
| `NonTxnDrop`, `CSN_MAX = 34`, `TID_MAX = 2` | killed, still growing, model defect `M15` | 56,703,779 after 9 min 30 s, queue 3.39M | |
| `NonTxnDrop`, `CSN_MAX = 34`, `TID_MAX = 2`, with `NonTxnDropView`, re-measured in task 5 | killed, still growing | 32,818,006 after 5 min, queue 2.71M | |
| the same, with a `CONSTRAINT` of one non-transactional query per behaviour | killed, still growing; lever rejected and reverted | 32,216,335 after 5 min, queue 2.70M | |
| `NonTxnDrop`, `CSN_MAX = 34`, `TID_MAX = 1`, **committed** | **green** | 1,112,076 | 11 s |
| `NonTxnDropTwo`, `CSN_MAX = 34`, `TID_MAX = 2`, no cleanup group, **committed** | **green**, queue peaked at 1.21M and drained | 47,958,711 | 7 min 34 s |
| `NonTxnInsert`, `CSN_MAX = 34`, **committed** | **green** | 15,787,889 | 2 min 33 s |
| `NonTxnF4` | RED on `NoLostVisibleData` | 74,225 | 2 s |
| `NonTxnF5` | RED on `ActiveSetShape` | 106,922 | 1 s |
| `NonTxnF6`, one session | RED on `Assert_validateInfo` | 113,093 | 1 s |
| `NonTxnF7` after the `M16` ghost fix | **no violation**, the module is deleted and F7 is withdrawn | 75.3 million after 600 s | |
| `NonTxnFixed`, one session, `OBSOLETE_IS_ROLLED_BACK = TRUE` | **green**, which is finding F6's fix verified | 841,907 | 8 s |
| `Base`, after this task | green | 26,839,136 | 3 min 56 s |
| `Merge`, after this task | green | 5,196,830 | 51 s |
| `SetSnapshot`, re-measured after `M13` | green | 12,766,799 | 1 min 55 s |
| `SetSnapshotFixed`, re-measured after `M13` | green | 12,236,834 | 1 min 52 s |
| `SetSnapshotF2Fixed`, re-measured after `M13` | green | 367,183 | 4 s |

`NonTxnInsert`'s count is reproducible to the multi-worker counting noise the run table already documents: a
second run of the committed configuration gave 15,787,914 and the closing re-run of the tree this is committed
with gave 15,787,838, a spread of 76 states across three runs.

The two `SetSnapshot` rows are the re-measurement model defect `M13` forces. Restoring `DropLock`'s `lockParts`
guard narrows the scenario by about 6%, from 13,622,631 and 13,092,635, which is the same direction and roughly
the same size as the move it made in `Base`. Both are still green, and the whole `SetSnapshot` witness sweep was
re-run with them; `WITNESSES.md` carries the verdicts, which are unchanged.

`Base` moves by 6%, from 28,547,508 to 26,839,136, and the move is real rather than noise. Two changes of this
task touch it. Model defect M13 restored `DropLock`'s `lockParts` guard, which `Base` had lost entirely because
its `Tasks` is empty, and a guard can only remove states. The visibility oracle gained `h.abandoned`, which
makes a statement-rolled-back part invisible to its creator as the code makes it, and which both removes states
and adds a view field. `Merge` does not move at all, to the state: its `Tasks` is a singleton, so the
quantifier defect was invisible there, and its count is 5,196,830 before and after.

### The F2 probe with a non-transactional creator {#nontxnf2}

`MC_NonTxnF2` is the insert half plus `SET TRANSACTION SNAPSHOT` and the cleanup thread, at one session, one
part, `TID_MAX = 2`, `CSN_MAX = 35` and `SNAPSHOT_TARGETS = {33, 34}`. It exists to pay debt B2, and its state
space is 9,132 distinct states at the first violation, which is small because the shape is sequential: the
creator is a query rather than a transaction, so the behaviour needs one session issuing an `INSERT`, a
transactional `DROP PARTITION` and a reader in turn.

Its view is `SetSnapshotView` verbatim, which is sound here for the same two reasons that view gives: `Covers`
is empty in this module, so a read's fragments are a function of its parts, and no enabled action writes a
field it leaves out. The batch fields are among those left out because there is no `DROP PARTITION` without a
transaction here and therefore no batch.

### Properties this scenario does not check, and why {#nontxn-properties-absent}

Four of `Base`'s properties are not in either half's cfg, and none of them is absent for convenience.

`StableRead` is falsified on the baseline by a non-transactional `INSERT` between two reads of one transaction,
which is real and by design. `NoLostRead` and `NoFutureRead` are falsified by a non-transactional write landing
during a read, which is model defect M14: both compare a read against the oracle in the state where the read
finishes rather than the state where it started. `NoLostVisibleData` is falsified by the removal batch and is
finding F4, shown in `MC_NonTxnF4`. Spec defect S13 is the qualifier the isolation section owes; the scenario
matrix's `NonTxn` row already names none of the four.

Two more come out for findings of their own, each with a module that produces it: `ActiveSetShape` for F5
(`MC_NonTxnF5`; it is in the insert half's roster, where it is vacuous because part `E` is never created there,
and `WITNESSES.md` carries the measured green that says so) and `Assert_validateInfo` and `NoAvoidableTermination` for F6 (`MC_NonTxnF6`, which checks both,
because the same violating record is an assertion inside `NOEXCEPT_SCOPE` and therefore a process termination as
well). Of the two only F6 has a fix verified in the tree, and both halves are therefore run with
`OBSOLETE_IS_ROLLED_BACK = TRUE`, the variant `MC_NonTxnFixed` verifies green at one session; without it the
baseline is unrunnable as a roster, because F6 stops every run on the first property that fires.

`ActiveSetShape` is in `MC_NonTxnInsert.cfg` and in neither of the two drop configurations. F5's routes all need the empty
covering part that only a non-transactional `DROP PARTITION` writes, so the property is unfalsifiable in the
insert half and live and unfixed in the drop half. The three batch properties are the mirror image: they are in
both rosters, and in `MC_NonTxnInsert` they are vacuous, because that half has no batch. They are kept there so
that the two cfgs differ in exactly the one row that has a reason.

`Atomicity` is checked in every configuration of this scenario. It was the property finding F7 was reported on,
and the withdrawal of that finding leaves its **baseline** green everywhere here, for the reason `FINDINGS.md`
gives: it is stated over a committed writer, and a non-transactional statement has none. Its witness is a
different matter and is red in both halves, at 168,614 distinct states in `MC_NonTxnDropTwo` and 486,689 in
`MC_NonTxnInsert`, so the property is falsifiable here and the green is a result rather than a vacuity.

## Reproducing {#reproducing}

```bash
./utils/tla/transactions/run_tlc.sh BaseSmall
./utils/tla/transactions/run_tlc.sh Base
```

Each writes `tmp/tla/<Scenario>/tlc.log`, keeps its metadir under `tmp/tla/<Scenario>/states` only while it runs,
and exits 0 when green. To re-measure a variant, copy the modules to a scratch directory under `tmp/`, edit the
copy, and run TLC there with its own `-metadir`; nothing under `tmp/` is committed.
