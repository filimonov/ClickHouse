# Findings of the TLA+ model of MergeTree transactions {#findings}

What the model found, and what happened to each finding. Three kinds are recorded here and nowhere else:
counterexamples TLC produced on the baseline scenarios, defects in the model itself that were noticed while
building it, and rows of the design document that the model contradicts.

A finding is classified `code`, `property` or `model`.

- `code` — the counterexample is a behaviour the C++ really has. The entry carries the counterexample, a fix
  applicable to upstream `master`, naming file and function and what changes, and the model variant that encodes
  the fix so the property can be rechecked against it.
- `property` — the C++ produces exactly what the trace shows, and the property misstated what it meant to
  forbid. The entry says what was wrong and what the property now says. A property is never weakened to make a
  red go away; if the design document's row was the thing that was wrong, section 3 records that too.
- `model` — the model let something happen that the code refuses. The entry names the C++ guard the model was
  missing.

Baseline C++ throughout is upstream `master` `2c24b6b9291e`, checked out in this worktree.

## 1. Counterexamples on the baseline {#counterexamples}

| Id | Scenario, bounds | Property | Action sequence (short) | Classification | Resolution | Proposed code fix |
|---|---|---|---|---|---|---|
| F1 | `Base` at `TID_MAX = 3`, `CSN_MAX = 38`, the first run at the transaction count the scenario matrix asks for | `Atomicity` | `t1` inserts `P1` and commits at CSN 34; `t2` takes snapshot 34, drops `P1`, inserts `P2`, and `CommitCreateCSN` writes CSN 35 into the log, which the updater loads while `t2` is still storing; `t3` takes snapshot 35, locks and stores its own removal of `P2`; `t3`'s `SELECT` then reads `{}` | `property` | `AtomicityStep` and the `ReadOK` mirror now exclude the reader's own removals, `h.removing[t]`, from `C`. The design document's row states only the writer's half of the same precedence, recorded in section 3 as S1 | none, the code is right |

### F1 in full {#f1}

Trace: `traces/atomicity-own-removal.txt`, 50 states, found after 7,982,042 distinct states.

In the final state `h.committed = {1, 2}`, `h.creating = <<{P1}, {P2}, {}>>`, `h.removing = <<{}, {P1}, {P2}>>`,
`h.csn[2] = 35`, `txn[3].snapshot = 35`, and `client[k1].last_read.parts = {}`. The reader is `t3`. Take the
committed writer `u = 2`: `h.loaded[2]` holds and `h.csn[2] = 35 <= 35`, so the clause applies. `C` was
`h.creating[2] \ h.removing[2] = {P2}`, unfiltered because `h.removers[P2]` is empty, and `Rm` was `{P1}`. With
`V = {}` neither branch holds: `C \subseteq V` fails on `P2`, `Rm \subseteq V` fails on `P1`. Red.

Both parts are invisible to `t3`, and both for reasons the C++ spells out.

`P1` is invisible because a committed removal precedes the reader's snapshot. Its record is
`ctid 1, ccsn 34, rtid 2, rcsn 0`. `VersionInfo::isVisible`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.cpp:138`) falls through its fast path and returns
`std::nullopt`, so `VersionMetadata::isVisible`
(`src/Interpreters/MergeTreeTransaction/VersionMetadata.cpp:46`) looks the removal TID up.
`TransactionLog::getCSN(removal_tid)` answers 35, because `t2` has already written its log entry, and the
closing expression at `VersionMetadata.cpp:100` requires `snapshot_version < current_removal_csn`, which
`35 < 35` fails. That is correct and intended: as far as the log is concerned `t2` is committed, and the log is
the only thing the reader can ask.

`P2` is invisible because the reader is itself removing it. Its record is `ctid 2, ccsn 35, rtid 3, rcsn 0` and
`t3` is the reader, so `VersionInfo::isVisible` reaches the own-removal clause first:

```cpp
    if (creation_csn && snapshot_version < creation_csn)
        return false;
    if (removal_csn && removal_csn <= snapshot_version)
        return false;
    if (!current_tid.isEmpty() && removal_tid == current_tid)
        return false;
```

`VersionInfo.cpp:167-172`. The third test fires and returns `false` before control ever reaches the creation
clause at `:178-183`, where `creation_csn = 35 <= 35` with an empty `removal_tid` would have made the part
visible. Own removal wins over visible creation. That is the same precedence `ReadYourWrites` already states,
and the same one the design document's `Atomicity` row already cites for the writer's own removals.

The remaining question was whether the model should have let `t3` lock `P2` at all, with `t2` still parked in
`CommitStoreCreation` and not yet done writing `ccsn` onto everything it created. It should. A transactional
removal runs `MergeTreeTransaction::removeOldPart` (`src/Interpreters/MergeTreeTransaction.cpp:213`), which
calls `lockRemovalTID` and then `setAndStoreRemovalTID` with no check on the creator at all. The one
creation-committed refusal nearby is on the non-transactional path only: the
`tid.isNonTransactional() && isCreatedByUncommittedTransaction()` guard in `setAndStoreRemovalTID`
(`VersionMetadata.cpp:172`) and the matching throw in `NonTransactionalRemovalLocks::lock`
(`MergeTreeTransaction.cpp:139`). Even that guard would not have fired here, because
`isCreatedByUncommittedTransaction` (`VersionMetadata.cpp:137`) asks `TransactionLog::getCSN(creation_tid)`,
which already answers 35 for `t2`, so the creation counts as committed. `lockRemovalTID`
(`VersionMetadata.cpp:195`) itself refuses only on a non-zero `removal_csn` or a lost compare-and-exchange, and
`P2` had neither.

So the read of `{}` is what the server returns, and the property was the thing that was wrong. `C` is the set of
`u`'s creations the reader must see, and a part the reader is dropping in its own transaction is not one of
them, for exactly the reason the row already gives for `u`'s own removals. The clause now reads

```
C == { p \in h.creating[u] \ h.removing[u] :
         /\ ~\E r \in h.removers[p] \ {u} : h.csn[r] <= s
         /\ p \notin h.removing[t] }
```

in both `AtomicityStep` and the `ReadOK` mirror. Nothing else moved. The `Rm \subseteq V` branch, the
`V \cap Rm = {}` branch and the `V \cap (h.creating[u] \cap h.removing[u]) = {}` conjunct are untouched, and the
`Atomicity` witness is still red, so the property still has teeth.

## 2. Model defects and where they get fixed {#model-defects}

| Id | Defect | Effect if left | Placement |
|---|---|---|---|
| M1 | `LegacyPartRecord` gives a legacy part `mem.sv = 0` (`Parts.tla:26`) while its disk record is `Legacy` (`Disk.tla:20`), which is not `Info`, so `DiskHasInfo` is false (`Disk.tla:42`) and `StoredRecord` falls back to `EmptyInfo` with `sv = -1` (`Parts.tla:128`) | Every store on a legacy part compares an expected `-1` against a tentative `sv` of 0 in `StorePersistStep` (`Parts.tla:153`), takes the `TOO_OLD_VERSION` branch, retries `MAX_STORE_RETRIES` times and ends in `STALE_VERSION`. No legacy part can ever be written, so plan 3's `Crash` runs with a non-empty `LEGACY_PARTS` would be vacuous | plan 3, the task that enables `LEGACY_PARTS` |

The C++ settles which side of the comparison is wrong. A part with no `txn_version.txt` is loaded by
`VersionMetadataOnDisk::loadMetadata` (`src/Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.cpp:49`),
which on the no-file path returns a default-constructed `VersionInfo` carrying only
`creation_tid = Tx::NonTransactionalTID` and `creation_csn = Tx::NonTransactionalCSN`. Its `storing_version`
keeps the member default, `VersionInfo::UNSTORED_VERSION = -1`
(`src/Interpreters/MergeTreeTransaction/VersionInfo.h:20,23`). The other side of the comparison,
`getExpectedStoringVersionUnlocked` (`VersionMetadataOnDisk.cpp:247`), also returns `UNSTORED_VERSION` when the
file does not exist. Both sides are `-1`, and the first store goes through.

So the model's `mem` is the wrong side, not `StoredRecord`. The fix is to drop the `!.sv = 0` from
`LegacyPartRecord` and let it keep `EmptyInfo`'s `-1`, which is what `loadMetadata` produces. Giving the disk
record a stored `sv` of 0 instead would contradict the C++, where a legacy part has nothing persisted at all.

## 3. Spec defects {#spec-defects}

Rows of `docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` that the model contradicts,
with the correction the next revision folds in.

| Id | Spec row | What is wrong | Correction |
|---|---|---|---|
| S1 | `Atomicity`, section "Invariants and properties" | `C` is built from the parts the writer created and did not itself remove, minus those a later committed transaction visible to the reader removed. It never subtracts the reader's own uncommitted removals, although the precedence it cites for the writer, own removal over visible creation, is the reader's too: `VersionInfo::isVisible` returns false on `removal_tid == current_tid` (`VersionInfo.cpp:171`) before reaching the creation clause at `:182`. Counterexample F1 | Add to `C` the condition that `p` is not in the reader's own `h_removing`. `Atomicity` in `Invariants.tla` already carries it |
| S2 | the `NoAvoidableTermination` witness row | The row names scenario `Base`, but the witness it describes needs a `Fail` inside `afterCommit` that takes the server down, which requires `ProcessDown`, an action `Base` does not enable. `WITNESSES.md` defers the witness to plan 5. One of the two statements has to give | Move the row's scenario to the first one that enables `ProcessDown`. The property itself stays in `Base`, where it is checked but not yet falsifiable |

### A spec row that was wrongly doubted {#withdrawn}

The `validateInfo, removal` witness row was on this list until the bounds changed, and it comes off it.

The row names one change, "`DropStore` skipped". At `TID_MAX = 2` that change alone left the run green, and the
witness was built as a two-change one, the second change stopping `EnrolBody` (`Server.tla:189`) from starting
the removal-TID store at all. `WITNESSES.md` described the two-change witness and both of its minimality halves
were green, so the row looked wrong.

At `TID_MAX = 3` the half that applies the wait change alone is red: 53,133,545 distinct states, and the trace
is exactly the shape the row names. `t2` locks `P1`, starts the removal-TID store, and returns from
`removeOldPart` without waiting for it; `t2` is then rolled back, which clears the removal TID; `t3` takes the
lock, commits, and `CommitStoreRemoval` computes a removal CSN of 35 for a record whose `rtid` is still empty,
which `validateInfo` rejects. Three transactions are the fewest that can build it, which is why two changes
looked necessary at two.

So the row was right and the model's witness was over-specified. `Assert_validateInfo_removal` is now the
one-change witness the row names, the `_only1` and `_only2` hooks are gone with the minimality check they
existed for, and nothing is owed to the next spec revision here.

### Two properties admitted without a spec row {#properties-without-a-row}

`RollbackNoLeak`, added by task 1, and `KillerNotStranded`, added by task 3, are both in `MC_Base.cfg`'s
`INVARIANTS` list, and neither appears anywhere in the design document: not as a property, not as a witness row.
The witness contract says a property without a passing witness is not accepted into `Invariants.tla`, so these
two were admitted against it.

The ruling recorded here is that they stay. Each states something the model would otherwise not check at all,
each holds on every baseline run, and removing them would lose coverage to satisfy a bookkeeping rule. What the
next spec revision owes them is a row each, with the witness that row implies:

- `RollbackNoLeak` — a witness is writable within `Base`: a rollback that leaves a created part visible to
  another session's read. It belongs with the plan that next touches the rollback actions.
- `KillerNotStranded` — the witness needs a rollback step that starts and never completes, so that a killer
  parked at `KillWait` is never released. `Base` has no such step, so this one goes to plan 5, alongside the
  other liveness witnesses.

Until those rows exist, both properties are checked without ever having been shown to be falsifiable. That is a
debt, recorded here and in the gap table of `WITNESSES.md`, not a result.
