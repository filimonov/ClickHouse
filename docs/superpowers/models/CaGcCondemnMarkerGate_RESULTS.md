---
description: 'TLC evidence for the condemn-marker gate: PR1 configuration, sabotage rows and the regression battery'
sidebar_label: 'CAS condemn-marker TLC results'
sidebar_position: 5
slug: /superpowers/models/CaGcCondemnMarkerGate-results
title: 'CAS condemn-marker TLC results'
doc_type: 'reference'
---

# `CaGcCondemnMarkerGate` results {#cagccondemnmarkergate-results}

## Verdict {#verdict}

The PR1 configuration (gate always reads, no registry, retry stamps the current round, job deletes only at a round up to the entry's) is green in every required green row. Each sabotage row (`rv_bug`, `f1_bug`, `pm_g1_late_resolved_entry`, `pm_g1_nogateround_noregistry`, `pm_g1_nojob`, `pm_g1_norestamp_live`) violates the invariant named for it. Violation rows differ in count between runs because TLC stops at the first counterexample, which depends on worker scheduling.

Three violation rows are accepted residuals, not sabotage: `pm_g1_late_resolved_stale` and `pm_g1_late_gaveup` (a late Clean request that still matches the marker, spec sections 10.1 and 10.2 of `docs/superpowers/specs/2026-09-30-cas-gc-meta-delete-names-its-condemnation-design.md`) and `pm_g1_extdrop_noregistry` (an externally dropped edge, same spec, section 10.1). They show only that the model can find the loss PR1 does not close. `pm_g1_late_resolved_version` is green: it is the same schedule family with `EtagIsContent` FALSE (GCS), where the late request does not match recreated bytes.

CAS-360: with the adopter's `HEAD` and marker `GET` split (`SplitAdopt`), today's protocol loses data (`cur_headget`, `violation:NoDangle`). Candidates A (deferred marker delete), B (second `HEAD`) and C (republish on absent marker) are green; each sabotage row violates `NoDangle`. See [CAS-360](#cas-360).

## Checker and finite scope {#checker-and-finite-scope}

- TLC `2026.07.18.145032`, revision `30cc360`; `tla2tools.jar` SHA-256 `cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3`.
- 8 workers, `nice -n 10`, `-Xmx8g`, deadlock checking on. PR1 rows use `MaxGen 3`, `MaxRounds 7`, `TwoWriters`; regression rows keep their own values.
- Not shown: the read-ahead, a destructive request landing after a PR1 read, an old build's pending entries, and GC's own ambiguous writes. `GSettleGraduate` makes the gate read, the graduation and the round commit one step.
- Not shown: the code's refuse-and-retry after a failed marker read when the marker already guards the entry (`GSettleCarryRetry` refuses only when `GateOK` is false), more than one hash, more than one retired entry, more than two writer slots, more than one `GStaleAttempt` and one late request per behaviour, and liveness. The meta value is its bytes, so under `EtagIsContent` it stands for S3's content etag.

## Switches {#switches}

| Constant | PR1 value | Meaning |
|---|---|---|
| `GateOnMarker`, `GateAlwaysReads`, `GateRoundCheck`, `JobRoundCheck`, `Restamp` | `TRUE` | the gate reads the marker; round check; job round check; re-stamp |
| `RetryStampsCurrent` | `TRUE` | the gate's retry write stamps the refusing round, not the entry's |
| `SplitMetaDelete`, `SplitPublish`, `RealCandidate`, `Respawn` | `TRUE` | the code's shape |
| `Registry`, `RegRound` | `FALSE` | no in-process registry |
| `PersistConf` | either | the carried bit; `GateAlwaysReads` ignores it |
| `WriterLeavesMarker`, `DeleteReads`, `GateTouched`, `ForgetOnTouch`, `ExternalDrop` | `FALSE` | rejected candidates |
| `OutstandingReconcile` | `"none"` | `"resolved"` / `"gaveup"`: a reconcile leaves one Clean If-Match request in flight |
| `EtagIsContent` | `TRUE` | S3: equal bytes match; `FALSE`: only an unreplaced version matches (GCS) |
| `TrackRetry` | `FALSE` | records a completed retry write for `NoWedgedCarry` |
| `MarkerPrefetch` | `FALSE` | `TRUE`: the gate decides on a copy of the marker taken after the cut and before the gate (the graduation read-ahead of `CasGc.cpp`, `GraduationReadHints`); `GObserveMeta` takes the copy, `SeenMeta` is what `GateOK` tests. Issue #2486 |

Fault switches: `AsyncMarker`, `Prefetch`, `ReconcileFault`, `StaleMarkers`, `StaleAttempts`, `SuppressRounds`. `ExactAbsentPath` makes publication only into a key with neither body nor meta.

## Invariants {#invariants}

```tla
NoDangle == \A w \in Writers : committed[w] => body.present

MarkerPreservedPending == (ret.st = "pending" /\ TokCurrent) => Cond
MarkerPreservedConfirmed == (ret.st = "condemned" /\ GateConfirmed /\ TokCurrent) => Cond

JobWouldDelete(c) == ~JobRoundCheck \/ (Cond /\ meta.cr <= c)
JobSparesPendingGuard == \A c \in mdq : ~(ret.st = "pending" /\ TokCurrent /\ JobWouldDelete(c))

NoWedgedCarry == ~(ret.st = "condemned" /\ ret.retried /\ Cond /\ meta.cr < ret.cr)
```

`NoDangle` compares content presence, not tokens. `MarkerPreservedConfirmed` is vacuous under `GateAlwaysReads` and is kept for the regression cfgs. `NoWedgedCarry` checks the re-stamp, not eventual graduation.

## Required runs {#required-runs}

All cfgs are committed next to the module; `run_condemnmarker.sh` asserts the invariant by name. Counts come from the battery run before `Next` was split into one disjunct per action (a change that does not alter the state graph). Afterwards `bug`, `fix`, `pm_g1_noregistry`, `rv_bug` and `pm_g1_nojob` were rerun; the green counts are identical (`fix` 845, `pm_g1_noregistry` 199,936). `pm_g1_nojob` was rerun after its cfg lost the `StaleMarkers` switch and is the only row with a different count here (38,082 distinct at the first counterexample). The last two rows are the earlier sabotage cfgs rerun on the new module. The trace column was read from the counterexamples as state diffs.

| Cfg | Expected | Observed | Distinct | Seconds | Trace |
|---|---|---|---:|---:|---|
| `pm_g1_noregistry` | green | `green` | 199,936 | 1 | - |
| `pm_g1_all_noregistry` | green | `green` | 6,089,253 | 21 | - |
| `rv_g1_noregistry` | green | `green` | 536,095 | 3 | - |
| `pm_g1_f1` | green | `green` | 4,578,945 | 15 | - |
| `pm_g1_live` | green | `green` | 386,063 | 2 | - |
| `pm_g1_late_resolved` | green | `green` | 675,800 | 2 | - |
| `pm_g1_late_resolved_entry` | violation | `violation:NoDangle` | 380,644 | 1 | the carried entry is cr2 and the retry stamps the entry's round; a reconcile resolves Clean and leaves a request expecting Condemned(cr2); the retry recreates identical bytes Condemned(cr2); the entry graduates on it, then the late request lands (equal bytes) as Clean and the redelete removes the body under a landed edge. |
| `pm_g1_late_resolved_version` | green | `green` | 13,862,134 | 47 | - |
| `pm_g1_mpf_noregistry` | green | `green` | 370,480 | 2 | `pm_g1_noregistry` with `MarkerPrefetch`: the read-ahead copy decides, no loss |
| `rv_g1_mpf_noregistry` | green | `green` | 1,913,847 | 9 | `rv_g1_noregistry` with `MarkerPrefetch` |
| `pm_g1_mpf_late_resolved` | green | `green` | 1,147,984 | 4 | `pm_g1_late_resolved` with `MarkerPrefetch` |
| `pm_g1_late_resolved_stale` | violation | `violation:NoDangle` | 542,881 | 2 | residual. A stale marker write Condemned(cr1) is the version the reconcile read; the reconcile resolves Clean, its request stays in flight; the round-1 condemn's own marker job (not a retry) recreates identical bytes Condemned(cr1); the entry graduates, a second writer's edge lands, the late request lands Clean and the body is deleted under a landed edge. |
| `pm_g1_late_gaveup` | violation | `violation:NoDangle` | 298,083 | 1 | residual. The reconcile faults and gives up with a request in flight expecting Condemned(cr2); the entry graduates to pending on that unchanged marker; the late request lands Clean; the redelete removes the body under a landed edge. |
| `pm_g1_nogateround_noregistry` | violation | `violation:NoDangle` | 4,010,406 | 12 | gate without the round check accepts a stale Condemned(cr1) for an entry of round 4; the stale delete job removes then a writer's Clean lands; entry deletes the body under a landed edge. |
| `pm_g1_nojob` | violation | `violation:JobSparesPendingGuard` | 38,082 | 1 | `JobRoundCheck` off, the only constant that differs from `pm_g1_noregistry` (its invariant list is shorter); a queued job captured at round 1 would delete the Condemned(cr4) marker guarding the pending entry (the invariant fires one step before the delete). |
| `pm_g1_norestamp_live` | violation | `violation:NoWedgedCarry` | 21,959 | 0 | `Restamp` off; a writer republishes the body over Condemned(cr1) with a faulted reconcile; the entry is superseded at round 2 and its marker job leaves Condemned(cr1) in place; the retry write at round 3 completes (`retried`) and the marker is still older than the entry's round 2. |
| `pm_g1_extdrop_noregistry` | violation | `violation:NoDangle` | 46,592 | 1 | residual. The first writer's edge is dropped externally while its reconcile is owed; a second writer's edge lands; the first writer's owed reconcile then writes Clean over Condemned(cr2) and the entry deletes the body under a landed edge. |
| `pm_g1_mpf_extdrop` | violation | `violation:NoDangle` | 85,254 | 1 | the same residual with `MarkerPrefetch`: the read-ahead adds no loss class. Every green row above stays green with the flag (`*_mpf_*`); the only violation it reaches is the externally dropped edge, which the marker read at the gate does not close either. |
| `rv_bug` | violation | `violation:NoDangle` | 14,238 | 1 | a publisher's Clean overwrites the entry's Condemned(cr1) marker; the in-process confirmation lets the entry graduate; body deleted while a writer's edge landed. |
| `f1_bug` | violation | `violation:NoDangle` | 1,299,275 | 3 | stale job deletes the marker, a new writer publishes over the absent marker and cleans, the entry (cr4) graduates on the old in-process confirmation and deletes the body under a landed edge. |

## Regression {#regression}

### Reproducible with committed cfgs {#regression-committed}

Promoted cfgs against the last committed RESULTS (`4dd5f1ac11b`). The module was replaced, not extended: the earlier module had different writer actions, so the counts moved while every result stayed the same.

| Cfg | Last committed distinct | Now distinct | Result |
|---|---:|---:|---|
| `fix` | 808 | 845 | `green` |
| `fix_pconf` | 768 | 803 | `green` |
| `fix_prefetch` | 1,337 | 1,385 | `green` |
| `fix_both` | 1,285 | 1,331 | `green` |
| `fix_both_2w` | 20,574 | 22,171 | `green` |

### Recorded counts, cfgs not committed {#regression-recorded}

Earlier battery rerun with the four constants at their inert values. Its cfgs are not committed, and several names (`fix`, `fix_both`, `rv_bug`, `f1_bug`) denote different configurations from the committed cfgs of the same name, so these rows are recorded counts, not reproducible from this directory. Green rows match in distinct states. Violation rows are first-counterexample counts and vary between runs. Rows that were not green before (`pm_g7_all`, `pm_g1w1_extdrop`, `pm_g7_big`, `pm_g1_big`) keep the same result as before.

| Cfg | Before: result | Before: distinct | After: result | After: distinct |
|---|---|---:|---|---:|
| `atomic_regress` | `green` | 20,802 | `green` | 20,802 |
| `bug_gateround` | `violation:NoDangle` | 52,084 | `violation:NoDangle` | 51,468 |
| `bug_jobdelay` | `violation:NoDangle` | 25,886 | `violation:NoDangle` | 25,397 |
| `bug_restamp` | `violation:NoDangle` | 55,888 | `violation:NoDangle` | 54,907 |
| `bug_stale` | `violation:NoDangle` | 114,328 | `violation:NoDangle` | 119,983 |
| `fix_both` | `green` | 104,271 | `green` | 104,271 |
| `fix` | `green` | 52,645 | `green` | 52,645 |
| `fix_splitpub` | `violation:NoDangle` | 34,382 | `violation:NoDangle` | 34,979 |
| `fix_stale_big` | `green` | 2,591,915 | `green` | 2,591,915 |
| `fix_stale` | `green` | 719,379 | `green` | 719,379 |
| `job_only` | `green` | 40,197 | `green` | 40,197 |
| `job_only_stale` | `violation:NoDangle` | 294,797 | `violation:NoDangle` | 308,988 |
| `nogate_stale` | `violation:NoDangle` | 311,874 | `violation:NoDangle` | 296,958 |
| `norestamp_stale` | `violation:NoDangle` | 281,131 | `violation:NoDangle` | 301,088 |
| `old_bug_both_2w` | `violation:NoDangle` | 7,830 | `violation:NoDangle` | 4,298 |
| `old_bug_both` | `violation:NoDangle` | 1,068 | `violation:NoDangle` | 847 |
| `old_bug` | `violation:NoDangle` | 455 | `violation:NoDangle` | 455 |
| `old_fix_both_2w` | `green` | 22,171 | `green` | 22,171 |
| `old_fix_both` | `green` | 1,331 | `green` | 1,331 |
| `old_fix` | `green` | 845 | `green` | 845 |
| `old_fix_pconf` | `green` | 803 | `green` | 803 |
| `old_fix_prefetch` | `green` | 1,385 | `green` | 1,385 |
| `rv_bug` | `violation:NoDangle` | 17,866 | `violation:NoDangle` | 9,981 |
| `rv_bug_mp` | `violation:MarkerPreservedPending` | 3,177 | `violation:MarkerPreservedPending` | 2,471 |
| `rv_bug_async` | `violation:NoDangle` | 14,165 | `violation:NoDangle` | 10,372 |
| `f1_bug` | `violation:NoDangle` | 1,333,161 | `violation:NoDangle` | 1,280,796 |
| `f1_bug_mp` | `violation:MarkerPreservedPending` | 321,419 | `violation:MarkerPreservedPending` | 382,988 |
| `pm_bug` | `violation:NoDangle` | 90,777 | `violation:NoDangle` | 89,130 |
| `pm_bug_all` | `violation:NoDangle` | 1,641,510 | `violation:NoDangle` | 1,613,975 |
| `pm_overapprox` | `violation:NoDangle` | 61,647 | `violation:NoDangle` | 68,444 |
| `pm_g1_nogateround` | `violation:NoDangle` | 35,332,696 | `violation:NoDangle` | 44,130,658 |
| `pm_g1_extdrop` | `violation:NoDangle` | 73,907 | `violation:NoDangle` | 58,910 |
| `pm_g1_wit` | `violation:WitNoDeleteAfterRefusal` | 11,400 | `violation:WitNoDeleteAfterRefusal` | 12,760 |
| `pm_g7_mpc` | `violation:MarkerPreservedConfirmed` | 5,511 | `violation:MarkerPreservedConfirmed` | 3,551 |
| `pm_g7_naive` | `violation:NoDangle` | 164,032 | `violation:NoDangle` | 133,738 |
| `pm_w1_stale` | `violation:NoDangle` | 9,526,734 | `violation:NoDangle` | 4,130,740 |
| `pm_g2_all` | `violation:NoDangle` | 1,890,991 | `violation:NoDangle` | 2,966,783 |
| `pm_g1w1_extdrop_stale` | `violation:NoDangle` | 6,626,988 | `violation:NoDangle` | 3,361,495 |
| `rv_g1` | `green` | 1,951,931 | `green` | 1,951,931 |
| `f1_fix` | `green` | 37,162,840 | `green` | 37,162,840 |
| `pm_nosplit` | `green` | 37,162,840 | `green` | 37,162,840 |
| `pm_g1` | `green` | 241,547 | `green` | 241,547 |
| `pm_g1_all` | `green` | 108,547,117 | `green` | 108,547,117 |
| `pm_g1_noreg` | `green` | 108,547,117 | `green` | 108,547,117 |
| `pm_g1_overapprox` | `green` | 57,644,865 | `green` | 57,644,865 |
| `pm_g1_norestamp` | `green` | 79,572,691 | `green` | 79,572,691 |
| `pm_g7_all` | `violation:MarkerPreservedPending` | 180,871 | `violation:MarkerPreservedPending` | 121,690 |
| `pm_w1` | `green` | 1,532,050 | `green` | 1,532,050 |
| `pm_g1w1_extdrop` | `violation:MarkerPreservedPending` | 57,061 | `violation:MarkerPreservedPending` | 53,780 |
| `f1_fix_big` | `green` | 241,031,093 | `green` | 241,031,093 |
| `pm_g1_big` | `timeout` | not recorded | `timeout` | about 388,000,000 at the 1,200 s timeout |
| `pm_g7_big` | `violation:MarkerPreservedPending` | 261,366 | `violation:MarkerPreservedPending` | 148,701 |

## Big run {#big-run}

`CaGcCondemnMarkerGate_pm_g1_big_noregistry.cfg`: `green`, 25,807,236 distinct states, 90 s (cap 7200 s). It is not part of `run_condemnmarker.sh`; run it with TLC directly. The count was observed before `Next` was split per action and was not rerun after it.


## CAS-360: the adopter's `HEAD` and marker `GET` are two reads {#cas-360}

`PartWriteTxn::ensureBlobPresent` (`Pool/CasPartWriteTxn.cpp`) issues `op.head(key)`, then a separate `loadMeta` `GET`. On an absent marker it backfills `Clean` (`putMetaIfAbsent`, outcome not read) and returns `Observed` with no second `HEAD`. `WAdopt` checked `body.present /\ ~Cond` in one step, so the model could not see a body deleted between the two reads. Audit rows: `docs/superpowers/cas/2026-10-12-late-request-audit/objects/meta.md` S9, `objects/blob.md` S13.

### What was split {#cas-360-split}

`SplitAdopt = TRUE` replaces `WAdopt` with one step per request:

| Action | Code | Effect |
|---|---|---|
| `WHead` | `op.head(key)` | present: remember the token (`hd`); absent: publish |
| `WMetaRead` | `loadMeta` | `Clean`: adopt `hd`; `Condemned`: publish; absent: backfill (today), second `HEAD` (B), publish (C) |
| `WReHead` | B only: a second `op.head(key)` | same token: backfill; anything else: next loop iteration |
| `WBackfill` | `putMetaIfAbsent(Clean)`, then the `Observed` return | writes `Clean` only over an absent key; adopts `hd` regardless |

`WPublish` under `SplitAdopt` fires only on the writer's own decision (`wst = "pub"`), not on a fresh atomic look at the store. GC already had the marker delete as a later step than the body delete (`SplitMetaDelete`: `GSettleRedelete` queues `mdq`, `GMetaDeleteJob` runs `deleteConfirmedMeta`).

New switches, all inert (`FALSE`/`"none"`) in every pre-existing cfg:

| Constant | Meaning |
|---|---|
| `SplitAdopt` | the split above |
| `FixMode` | `"none"`, `"A"`, `"B"`, `"C"` |
| `FixGuard` | `FALSE` removes the candidate's decisive check (sabotage) |
| `AKeepOnSpare` | A: a spared body-gone entry is carried until a round at in-degree 0 deletes its marker |
| `GoneDeleteAnyRound` | A: the marker delete removes a `Condemned` marker of any round `< round`, not only `<= entry.condemn_round` |
| `MetaDeleteFault` | a marker delete may fail; today's queued job is then lost |
| `DeposedDelete` | once per behaviour, a deposed attempt's redelete removes a published `delete_pending` body at any time (today it also queues the marker delete) |
| `AGoneDropsNow` | A sabotage: a body already `Gone` at the first redelete pass deletes the marker in that round |

New invariant `OrphanTracked`: a `Condemned` marker over an absent body, with no landed writer edge and no queued delete job, is still owned by a retired entry. It is a leak proxy, not a safety property.

### Counterexample, current protocol {#cas-360-trace}

`cur_headget` (`pm_g1_live` with `SplitAdopt`), 15 states:

1. Seed body T1; round 1 cut sees it at zero, condemns T1; the async marker job writes `Condemned(1)`.
2. Round 2 cut; the gate reads `Condemned(1)` and graduates the entry to `pending`.
3. Round 3 cut, no edges folded.
4. Writer W precommits (after the cut), `HEAD` returns T1.
5. `GSettleRedelete` removes T1 (`Removed`) and queues the marker delete; round 3 commits.
6. `GMetaDeleteJob` deletes `Condemned(1)`.
7. W's `GET` finds no marker; `putMetaIfAbsent(Clean)` lands on the empty key; W adopts T1.
8. W commits: a committed edge to a deleted body (`NoDangle`).

The gate is not violated: the marker was `Condemned` until after the body delete. W's decision combines two reads taken at different times. `cur_deposed` reaches the same loss when a deposed attempt does the body delete.

### Candidates {#cas-360-candidates}

- **A, deferred marker delete.** The pass that deletes the body (`Removed`) or finds it `Gone` keeps the entry (`"gone"`, still `delete_pending`) and does not touch the marker. The next round settles it against that round's in-degree: `> 0` spares it with the marker left `Condemned`; `= 0` runs `deleteConfirmedMeta` synchronously and drops the entry only if the delete succeeded. Why it holds: W's precommit < W's `HEAD` < body delete < the next round's cut, so W's edge is in the in-degree of the round that would delete the marker. The marker is `Condemned` at every W `GET` before that round, and W republishes.
- **B, second `HEAD`.** On an absent marker, `HEAD` again and adopt only an unchanged token. Why it holds: the marker was absent at `t_g` and the same token was present at `t_h2 > t_g`. GC deletes the marker only after deleting the body, so the body was not deleted before `t_h2`, and any later delete needs a graduation read after `t_g`, whose round's cut folds W.
- **C, republish on absent marker.** An absent marker never adopts.

### Runs {#cas-360-runs}

TLC `2026.07.18.145032`, 4 workers, `nice -n 10`, `-Xmx8g`. Two writers, `MaxGen 3`, `MaxRounds 7`. Rows without `_all` use the `pm_g1_live` faults; `_all` rows use `pm_g1_all_noregistry` (adds `PersistConf`, `Prefetch`, `StaleMarkers`, `StaleAttempts`).

Pre-existing rows in the same battery run (`run_condemnmarker.sh`, all 51 rows, `ALL EXPECTATIONS MET`): every row keeps its result, and every green row keeps its distinct-state count (`fix` 845, `pm_g1_noregistry` 199,936, `pm_g1_all_noregistry` 6,089,253, `pm_g1_late_resolved_version` 13,862,134). The new variables are constant when `SplitAdopt` is off. No old green depended on the fused `WAdopt`, because no old cfg splits it.

| Cfg | Expected | Observed | Distinct | Seconds | Note |
|---|---|---|---:|---:|---|
| `cur_headget` | violation | `violation:NoDangle` | 27,530 | 1 | today's protocol: backfill over the deleted marker, adopt a deleted body (trace above) |
| `cur_headget_all` | violation | `violation:NoDangle` | 273,931 | 2 | same loss with every fault switch of `pm_g1_all_noregistry` |
| `cur_deposed` | violation | `violation:NoDangle` | 19,853 | 1 | same loss; a deposed attempt deletes the body and queues the marker delete |
| `sab_deferA` | violation | `violation:NoDangle` | 49,122 | 1 | A without the in-degree check: the marker is deleted in the next round although W is folded |
| `sab_deferA_gonenow` | violation | `violation:NoDangle` | 33,009 | 1 | A deleting the marker in the round whose first pass finds the body already `Gone` (deposed delete after the cut) |
| `sab_reheadB` | violation | `violation:NoDangle` | 50,532 | 1 | B with the second `HEAD` result ignored |
| `sab_republishC` | violation | `violation:NoDangle` | 30,000 | 1 | C adopting on an absent marker (today's rule) |
| `fix_deferA` | green | `green` | 2,694,270 | 15 | - |
| `fix_deferA_keep` | green | `green` | 2,774,035 | 16 | A + `AKeepOnSpare` |
| `fix_deferA_keepany` | green | `green` | 2,767,736 | 16 | A + `AKeepOnSpare` + `GoneDeleteAnyRound` |
| `fix_deferA_all` | green | `green` | 38,361,398 | 381 | - |
| `fix_deferA_keepany_all` | green | `green` | 39,623,012 | 381 | + `MetaDeleteFault`, with `OrphanTracked` not checked |
| `fix_deferA_deposed` | green | `green` | 4,199,367 | 22 | - |
| `fix_deferA_keepany_deposed_safe` | green | `green` | 4,369,916 | 23 | A + keep + any-round + `MetaDeleteFault` + `DeposedDelete`, safety only |
| `fix_reheadB` | green | `green` | 3,475,290 | 20 | - |
| `fix_reheadB_all` | green | `green` | 50,855,227 | 500 | - |
| `fix_reheadB_deposed` | green | `green` | 7,622,045 | 41 | - |
| `fix_republishC` | green | `green` | 3,504,272 | 20 | - |
| `fix_republishC_all` | green | `green` | 41,171,364 | 404 | - |
| `fix_republishC_deposed` | green | `green` | 6,934,001 | 37 | - |
| `cur_orphan` | violation | `violation:OrphanTracked` | 2,485 | 1 | today: a failed marker delete job leaves `Condemned` over no body, no entry |
| `cur_orphan_retry` | violation | `violation:OrphanTracked` | 11,738 | 0 | today, no delete fault: the retry stamps round 2, `deleteConfirmedMeta(cr 1)` skips it |
| `fix_deferA_orphan` | violation | `violation:OrphanTracked` | 11,353 | 1 | A drops a spared body-gone entry; the spared writer drops before republishing |
| `fix_deferA_keep_orphan` | violation | `violation:OrphanTracked` | 18,233 | 1 | A + keep: the `<= entry.condemn_round` bound skips the retry-stamped marker |
| `fix_deferA_keepany_orphan` | green | `green` | 2,767,736 | 16 | A + keep + any-round, with `MetaDeleteFault`: a failed delete keeps the entry, retried next round |
| `fix_deferA_keepany_deposed` | violation | `violation:OrphanTracked` | 6,515 | 1 | residual leak: a `pending` entry spared at in-degree > 0 after a deposed attempt removed its body |

### Findings beyond CAS-360 {#cas-360-findings}

- **Orphan marker in today's code, no fault needed besides a lost marker write** (`cur_orphan_retry`). The condemn marker write is lost; the gate refuses, its retry stamps the current round 2 (`confirm_condemned_marker` passes `condemn_round`, not `entry.condemn_round`); the entry graduates on `Condemned(2)`; after the body delete `deleteConfirmedMeta(ref, entry.condemn_round = 1)` skips a marker with `condemn_round 2 > 1`. The `Condemned` marker outlives its body. Safe (a writer republishes on `Condemned`), but nothing ever removes it. Under A, `GoneDeleteAnyRound` closes it (`fix_deferA_keepany_orphan` green).
- **A must be two-pass even when the first pass finds the body `Gone`** (`sab_deferA_gonenow`). A deposed attempt's pre-commit redelete can remove the body after this round's cut; deleting the marker in the round that first sees `Gone` reopens the race. The marker delete belongs to a round strictly after the pass that saw the body removed or gone.
- **A without `AKeepOnSpare` leaks a marker** when the spared writer aborts before republishing (`fix_deferA_orphan`). `AKeepOnSpare` keeps the body-gone entry until in-degree 0. A spared `pending` entry whose body a deposed attempt removed still leaks (`fix_deferA_keepany_deposed`); safety holds there (`fix_deferA_keepany_deposed_safe`).

### Recommendation {#cas-360-recommendation}

**A + `GoneDeleteAnyRound`**, two-pass, with B as the fallback if A cannot get its persisted bit.

- A is GC-side. It closes the race for every writer binary, including writers not yet upgraded in a rolling upgrade; B and C need every writer upgraded. A also gives the marker delete a retry (the entry is kept on failure), and with the any-round bound it closes the orphan in `cur_orphan_retry`.
- Code change for A: `Gc::applyRedeleteOutcome` on `Removed` or `Gone` keeps the entry `delete_pending` with a "body gone" mark and schedules nothing. `settleEntry` (`Gc/CasBlobInDegree.cpp`) already spares on `indeg > 0`. A marked entry at `indeg == 0` runs `deleteConfirmedMeta` before the round commits, and only a successful delete (or a marker that is not `Condemned`) drops it. `deleteConfirmedMeta` takes a bound `< current round` instead of `entry.condemn_round`.
- Cost for A: per deleted blob, +1 `HEAD` (second redelete pass) and +1 round of carry for the ledger row; the `GET` + `DELETE` of the marker moves from the async pool into the next round. No extra request on the write path.
- Cost for B: ~10 lines in `PartWriteTxn::ensureBlobPresent`, the `!loaded` branch; +1 `HEAD` only on the absent-marker path. Relies on token uniqueness for its argument.
- Cost for C: same branch; an absent marker costs a full body upload instead of a `PUT` of the marker. Absent markers occur on blobs whose marker create failed and in this race, so the volume is small, but each one is a body upload.
- `AKeepOnSpare` is optional: without it a spared writer that aborts before republishing leaves a `Condemned` marker over no body. The next writer of the hash republishes and reconciles it to `Clean`, so it is a leak only for hashes never written again.

### Not shown {#cas-360-not-shown}

- Token recurrence (CAS-361, a resent publication restoring an old token): not modelled here; see `CaBlobPublishCore`. B's argument uses token uniqueness.
- The `HEAD (expect gone)` of A's second pass is not a separate step; with unique tokens a body-gone entry's token does not come back.
- A redelete `Mismatch` on the second pass (a republished body) is folded into `deleteConfirmedMeta`'s conditional delete, which leaves the republisher's `Clean` alone.
