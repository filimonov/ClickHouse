# State-space budget for the `Base` scenario {#state-space}

`Base` runs two sessions (`Sessions = {k1, k2}`) over two parts with `TID_MAX = 2`. As first written it did not
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
new action `KillReturn` releases it once the transaction it drove no longer lists it as a holder.

**C. A rollback body has exactly one driver.** `MergeTreeTransaction::rollback` opens with a
`compare_exchange_strong` on `csn` and returns `false` to every caller that loses it, so the body that marks the
created parts, outdates them, restores the removed ones and unlocks their removal `TID` runs on exactly one
thread. The model added the killer to `holders` while leaving the owner there, and `Drives(k, t)` lets any holder
take the next rollback step, so the owner and the killer could alternate arbitrarily across the whole body. The
kill now sets `holders` to the killer alone, which is the caller that won the compare-and-exchange.

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
and still growing, and `TID_MAX = 2` already bounds the transactions that can reach `CommitAck` or
`RollbackFinalize` to two, because `Begin` draws from a monotone counter capped at `TID_MAX`, which makes that
constraint vacuous.

`CSN_MAX = 35` is a bound and is the smallest one that works: real commit sequence numbers start at
`FirstCSN = 33`, the log starts with one entry there, and two transactions can commit, so the sequence reaches
35. Lowering it would disable `CommitCreateCSN` for the second commit.

## Where `Base` stands {#final}

Both scenarios green, `-workers auto` on 32 cores, one run at a time.

| Scenario | States generated | Distinct states | Time |
|---|---|---|---|
| `BaseSmall` | 35,609 | 24,667 | 1 s |
| `Base` | 4,321,337 | 1,814,598 | 16 s |

Under `-workers auto` the `Base` counts move by a few states between runs, because two workers can fingerprint
the same state before either has inserted it; 4,321,334 and 1,814,597 came out of the run before this one.

`BaseSmall` is unchanged by all of the above: with one session no transaction is ever killed, so none of the
three corrections and none of the dropped fields make a difference to it.

## Reproducing {#reproducing}

```bash
./utils/tla/transactions/run_tlc.sh BaseSmall
./utils/tla/transactions/run_tlc.sh Base
```

Each writes `tmp/tla/<Scenario>/tlc.log`, keeps its metadir under `tmp/tla/<Scenario>/states` only while it runs,
and exits 0 when green. To re-measure a variant, copy the modules to a scratch directory under `tmp/`, edit the
copy, and run TLC there with its own `-metadir`; nothing under `tmp/` is committed.
