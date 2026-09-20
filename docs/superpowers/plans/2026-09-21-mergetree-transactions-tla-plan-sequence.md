---
description: 'The plan sequence of the MergeTree transactions TLA+ model campaign: the five implementation plans, their tasks by name, and the rule that every debt or parked defect names one of these tasks as its closer.'
sidebar_label: 'TLA+ transactions, plan sequence'
sidebar_position: 20
slug: /superpowers/plans/mergetree-transactions-tla-plan-sequence
title: 'MergeTree transactions TLA+ model: plan sequence and task names'
doc_type: 'plan'
---

# MergeTree transactions TLA+ model: plan sequence and task names {#tla-plan-sequence}

This document exists so that a placement in `utils/tla/transactions/FINDINGS.md` can name a plan and a task
that a future author will find. Plans 1 to 3 are written; plans 4 and 5 are named here and written after plan 3
executes, keeping these task names. A placement must read `plan N, task K (<name>)`. The campaign gate is that
`FINDINGS.md` section 2a is empty or every row points at a closing commit.

## Plan 1: foundation and the `Base` scenario (complete) {#plan-1}

`docs/superpowers/plans/2026-09-17-mergetree-transactions-tla-plan-1-foundation.md`. Tasks 1 to 5: install and
runner; witness harness; state-space budget; README; matrix bounds and the `Atomicity` clause.

## Plan 2: snapshots, cleanup, merges, non-transactional queries (complete) {#plan-2}

`docs/superpowers/plans/2026-09-18-mergetree-transactions-tla-plan-2-cleanup-merge-nontxn.md`. Tasks 1 to 5:
`SET TRANSACTION SNAPSHOT` and log truncation; the cleanup thread; merges and the task actor; non-transactional
queries and the removal batch; budget, witness sweep, debts.

## Plan 3: Keeper faults, crash and restart (written) {#plan-3}

`docs/superpowers/plans/2026-09-20-mergetree-transactions-tla-plan-3-keeper-crash-restart.md`. Task 1: Keeper
faults at commit, the unknown-state pass, updater-driven commit and rollback (produces `DrivesA(a, t)` and the
actor-parameterized `Rollback*A` steps). Task 2: the layered disk, `Fsync`, `Crash`, `ProcessDown`, the restart
loader. Task 3: restart safety properties (`NoResurrection`, `LogEntryNeeded`, `LegacyLoads`) and the legacy-part
fix M1. Task 4: `SnapshotCrash` and `NonTxnCrash` (the crash inside the plan-2 scenarios; the truncation pass
runs beside the batch here). Task 5: budget, witness sweep, documents, debts.

## Plan 4: mutations (to be written after plan 3) {#plan-4}

Task 1: mutation entries, `MutPrepareAttach`/`MutRegister`/`MutSelect`/`MutFinish`, the `Mutation` scenario.
Task 2: `KILL MUTATION` and the registration re-check (`MutationChain`, `NoOrphanMutation`, `NoUnknownMutationCSN`).
Task 3: mutation files under crash and restart (`writeCSN` unsynced append, `RestartLoadMutation`,
`MutationRecovered`/`MutationRecoveredStrict`, `MutationCrash`). Task 4: merges with mutations and the covering
relation in range shape (`MergeMutation`, `Covers` as a range; owner of M10). Task 5: isolation properties
revisited with mutations (`SelectCapture` ghost of the parts that were `Active`; owner of M14), `QueryFault`,
budget, witness sweep, documents, debts.

## Plan 5: disk faults, availability, liveness, calibration (to be written after plan 4) {#plan-5}

Task 1: disk write faults in the store frames, `StoreRetry`, `NoSpuriousStaleVersion` under faults, the
task-driven rollback machine (owner of M11). Task 2: `ProcessDown` policies `Terminate` versus `Retry`,
`NoAvoidableTermination`, `KillRetry`, `Implicit` (owners of M7 and M8). Task 3: liveness (`Live`,
`RetryProgress`, `OutdatedEventuallyDeleted`, the temporal-violation pattern in `witness.sh`, the
`KillerNotStranded` witness). Task 4: the budget and calibration task — larger budgets or scenario-specific views
for every witness row placed here (the B1/B3/B4 rows that need a third transaction at witness bounds), the
non-transactional batch beside `Updater+GC` (owner of M12), the state-space budget of `SetSnapshot` at matrix
bounds (owner of M4), the ordered encoding of `snapshots_in_use` if needed (owner of M6), and the calibration
re-run against the 23 upstream fixes. Task 5: final documents, spec revision, campaign gate (section 2a empty).
