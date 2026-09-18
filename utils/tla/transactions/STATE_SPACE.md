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
reduction was sought. The two factors are not separable by these runs: the actions and the two view fields
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

## Reproducing {#reproducing}

```bash
./utils/tla/transactions/run_tlc.sh BaseSmall
./utils/tla/transactions/run_tlc.sh Base
```

Each writes `tmp/tla/<Scenario>/tlc.log`, keeps its metadir under `tmp/tla/<Scenario>/states` only while it runs,
and exits 0 when green. To re-measure a variant, copy the modules to a scratch directory under `tmp/`, edit the
copy, and run TLC there with its own `-metadir`; nothing under `tmp/` is committed.
