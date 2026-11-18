---
description: 'TLC evidence for the offline migration of a CAS pool to blob deletion intents (clickhouse-disks cas-migrate-pool, M1 to M9): green configurations, admitted residuals R1 to R4 shown red, sabotages of the tool, witnesses, liveness, bounds.'
sidebar_label: 'CAS blob intents migration TLC results'
sidebar_position: 6
slug: /superpowers/models/CaBlobIntentMigration-results
title: 'CAS blob intents migration TLC results'
doc_type: 'reference'
---

# `CaBlobIntentMigration` results {#cablobintentmigration-results}

Model of §7 of `docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md` at commit `d16ec5f5eec`.
Step and rule ids (M1 to M9, W2 to W5, G1 to G8, I4 to I8, R1 to R4, S12, S13) are the specification's.
`CaBlobIntentMigration_<name>.cfg` holds configuration `<name>`.

## Verdict {#verdict}

**The migration passes at floors 1 to 3. Every red maps to a named residual; case (e) reaches further than the
text of §7.4 at `d16ec5f5eec` says, and R1 (e) is widened to cover it.**

- **Green with every older member stopped**, at floor 3 and at floor 1: every seed the argument of §7.4 admits (at
  floor 1 without the marker clause, case (a)), each kind of late older request, a crash after every step with
  reruns, the option on and off. A suspended GC leader and a suspended mounted writer whose clock kept running are
  green. A member that starts before the M7 read is detected: M7 refuses.
- **Floor 1.** M1 forces `--intents-for-all-bodies`; the raise to floor 3 is the first write of M6, after M5. A crash
  between M5 and M6 reruns with the option forced; a crash after the raise reruns without it, on the intents of the
  first run's M5. Raising the floor before M5 loses a blob: see [Sabotages](#sabotage-traces).
- **M5 covers a pending row's own token.** Case (e) no longer lets G4 delete a token with no confirmed intent:
  `mig_row_token_recur_i5` is green on I5, and red without the rule (`sab_mig_no_row_token_intent`).
- **Case (e), G4 of this build: R1 (e) as widened.** With one delayed token-repeating copy and no older delete, a
  ref this build commits by publishing loses its body to G4 of this build. See [Case (e), G4 of this
  build](#r1-recur-g4). Precondition: an older writer's copy still in flight after M8.
- **Case (e) and I4: R1 (e) as widened.** G8 sends no `HEAD`, so a token restored between G4 and G8 loses its
  intent while current. Precondition: an older writer's copy still in flight after M8.
- **No older request in flight, no loss.** The same seeds with case (e) on and `Late = {}` are green on every
  invariant (`res_r1_recur_no_late`).
- **Admitted residuals are red when allowed:** R1 (a) to (e), R3, R4, and R2 (resumed inside the mount sequence; a
  mounted writer whose clock stood still).
- **Sabotages.** Every tool sabotage is red. `markers_before_floor` (M9 before M8) is red only on the invariant that
  restates the rule; I8 stays green, see [Sabotages](#sabotages).

## Checker and how to run {#checker}

- TLC `2026.07.18.145032`, revision `30cc360`; OpenJDK 21.0.9; 8 workers; `-noGenerateSpecTE`; deadlock checking off.

```bash
java -Xmx16g -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -noGenerateSpecTE -workers 8 \
    -metadir <dir under tmp> -config CaBlobIntentMigration_mig_base.cfg CaBlobIntentMigration.tla
```

- Counts of runs that end in a violation depend on the worker count; counts of complete runs do not.

## Model {#model}

| Part | What it is |
|---|---|
| Seed (`Init`, `Consistent`) | The older world when the tool starts: `_pool_meta` floor 1, 2 or 3 (`SeedFloor`); body token 1 or no body; marker `absent`, `Clean`, `Condemned`; row none, candidate or pending on token 1 or on a replaced token 2; an older ref; late requests. `Consistent` is §7.4 "Which tokens a late delete can name": while `t` is current and under older delete authority (a pending row or a sent delete), the marker is `Condemned`, no marker request in flight can still change it, and no older writer adopted `t`. `Anomaly` drops one clause at a time. Below floor 3 the marker clause is dropped: case (a), a GC with no graduation gate |
| Late requests (§7.4) | GC `DELETE B If-Match t`; writer body `PUT` (fresh, or the recurring token under case (e)); writer `Clean` create or swap; GC `Condemned` write or marker delete; GC `gc/state` commit or lease write. Each lands at most once, at any time, or never |
| The tool (§7.1) | `TM1` to `TM9`, one action per step; M1 forces the option below floor 3; M2 is two reads with the fence; M5 a send, an answer and an unanswered branch with the resolve read, then `TM5p`, the create for a pending row's own token when the `HEAD` showed no body or another token; M6 starts with `TM6r`, the raise of a floor below 3. `TCrash` after any step, `TStart` reruns from M1. `Tools` runs at once |
| This build (§3) | Runs only after M8 (§2.3). Writer W2, W4 (two reads), W5; linker (§4.2, no request); GC: lease read and cut, one fold step (G2, G3, G4, G6, G7, G8), commit at header 4. G4 commits `standing(t)` and keeps the intent; G8 deletes it in a later round. G9 is not modelled: an orphan intent stays |
| Older member (§7.4 R2, R4) | Mount sequence (`_pool_meta` read, slot claim), slot renewal, `gc/hb`, a marker writer and a marker GC. `OldMember` picks the scenario |

### Invariants {#invariants}

| Invariant | Statement |
|---|---|
| `INV_COMMITTED_REF_HAS_BODY` | I8 unqualified: any committed ref (older, this build's, a copy) names a present body |
| `INV_REF_BY_OBSERVATION_HAS_BODY` | I8 as qualified in §4.1: a ref this build commits by W2 to W5, or a copy of one |
| `INV_DELETE_EVIDENCE` | I5 with G8: a body `DELETE` of this build has a confirmed intent for its token (G3's on the row, or M5's), a committed pending row and a later cut at in-degree 0 |
| `INV_I4_INTENT_PERMANENT` | no intent `DELETE` while its token is current |
| `INV_TOOL_DESTROYS_NOTHING_BEFORE_FLOOR` | §7.1 "Order": the tool deletes nothing before M8 |
| `INV_NO_LIVE_OLD_SLOT_AFTER_FLOOR` | A8 as M2, M7 and the fence enforce it |
| `INV_FORCED_OPTION_COVERS_SEED_BODY` | M1 and M6: on a pool that started below floor 3, a seed body still current after M8 has a confirmed intent |
| `INV_LOSS_IS_R1E` | every committed ref with no body lost it to an older delete of the recurring token: case (e) of R1 |
| `INV_I1_NO_RECURRENCE` | diagnostic; red only under case (e) |
| `LIVE_MIGRATION_COMPLETES` | a run reaches `migration complete` and the floor stays raised |

## Runs {#runs}

Defaults: one tool, no older member, every seed row, marker, body-absent and older-ref choice, no late request, no
anomaly, option off, floor 3, no fault, linker on, no crash, one run, one publication per build, 6 `gc/state` writes.
Configurations with an older member use smaller seeds: no older ref, no linker, a body present, rows `none` or
`pend`, markers `Clean` or `Condemned`, 4 `gc/state` writes, two runs, no late request except for the GC leader.

### Green {#green}

| Configuration | Differs from the defaults | Result | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `mig_base` | — | green | 120,574 | 48 | 1s |
| `mig_base_all_bodies` | option | green | 122,966 | 48 | 1s |
| `mig_crash_after_each_step` | 2 crashes, 3 runs, faults, late delete | green | 10,900,117 | 71 | 33s |
| `mig_crash_after_each_step_all_bodies` | the same, option | green | 11,113,995 | 71 | 35s |
| `mig_two_runs` | two tools, 1 crash, 3 runs, late delete | not re-run, see [Bounds](#bounds) | — | — | — |
| `mig_late_old_delete` | late delete | green | 364,559 | 49 | 2s |
| `mig_late_old_delete_all_bodies` | the same, option | green | 369,343 | 49 | 2s |
| `mig_late_old_put` | late delete, late `PUT` | green | 2,811,759 | 51 | 8s |
| `mig_late_old_marker_write` | late delete, `Condemned` write, marker delete | green | 2,228,913 | 50 | 7s |
| `mig_late_old_clean` | late delete, `Clean` create, `Clean` swap | green | 1,766,845 | 50 | 6s |
| `mig_late_old_gc_commit` | late delete, `gc/state` commit, lease write; 3 runs | green | 2,493,572 | 60 | 8s |
| `mig_all_late_no_crash` | every late kind; 3 runs; body present; no linker | green | 51,601,661 | 62 | 145s |
| `mig_spared_row_marker_only` | row none, `Condemned`, late delete | green | 22,349 | 47 | 0s |
| `mig_inherited_pending_executed` | row pending, late delete | green | 161,336 | 49 | 1s |
| `mig_member_started_before_recheck` | a member starts; its claim precedes a passed M7 | green | 15,976,639 | 80 | 74s |
| `mig_suspended_gc_leader` | a GC leader suspended in a round, late delete | green | 27,392,294 | 69 | 103s |
| `mig_suspended_writer` | a mounted member suspended; its clock keeps running | green | 5,576,768 | 67 | 15s |
| `live_migration_completes` | `LiveSpec`; 2 crashes, 4 runs; late delete, `gc/state` commit and lease, `Clean` create | not re-run, see [Bounds](#bounds) | — | — | — |
| `mig_floor1_base` | floor 1: the option forced, the marker clause dropped | green | 134,802 | 48 | 1s |
| `mig_floor2_base` | floor 2 | green | 134,802 | 48 | 1s |
| `mig_floor1_late` | floor 1; late delete, `Clean` create and swap, `Condemned` write, marker delete | green | 5,996,351 | 50 | 17s |
| `mig_floor1_crash_rerun` | floor 1; 2 crashes, 3 runs, faults, late delete | green | 21,775,162 | 71 | 68s |
| `mig_floor1_late_crash` | floor 1; the late kinds of `mig_floor1_late`; 1 crash, 2 runs, faults | green | 67,076,465 | 64 | 263s |
| `mig_row_token_recur_i5` | case (e) on, no older ref; I5 and the order invariant only | green | 5,527,329 | 51 | 15s |
| `live_floor1_completes` | `LiveSpec`; floor 1; 2 crashes, 4 runs; late delete | green | 6,035,789 | 70 | 201s |

`mig_rebuilt_state_marker_only` of §10.1 is the same seed as `mig_spared_row_marker_only` here: a row lost in a
rebuild and a spared row both leave no row and a `Condemned` marker.

### Admitted failing histories (§10.1 (b)) {#residuals}

Each is run against `INV_COMMITTED_REF_HAS_BODY` alone (the `_by_observation` rows: every invariant but that one).

| Configuration | Residual | Switch | Result | Distinct states | Depth |
|---|---|---|---|---|---|
| `res_r1_marker_anomaly` | R1 (a) to (c) | marker `absent` or `Clean` over a token under a late delete | RED `INV_COMMITTED_REF_HAS_BODY` | 1,349 | 20 |
| `res_r1_marker_anomaly_all_bodies_by_observation` | the same, option | — | green | 44,698 | 45 |
| `res_r1_late_clean` | R1 (b) | a late `Clean` swap on the current marker version | RED `INV_COMMITTED_REF_HAS_BODY` | 2,747 | 20 |
| `res_r1_late_clean_all_bodies_by_observation` | the same, option | — | green | 111,691 | 46 |
| `res_r1_missed_listing` | R1 (d) | M4's listing omits the key | RED `INV_COMMITTED_REF_HAS_BODY` | 1,318 | 22 |
| `res_r1_missed_listing_all_bodies` | the same, option | — | RED `INV_COMMITTED_REF_HAS_BODY` | 1,455 | 22 |
| `res_r1_token_recurs` | R1 (e) | a late `PUT` lands the replaced token | RED `INV_COMMITTED_REF_HAS_BODY` | 1,115 | 3 |
| `res_r1_token_recurs_all_bodies` | the same, option | — | RED `INV_COMMITTED_REF_HAS_BODY` | 1,107 | 3 |
| `res_r1_token_recurs_by_observation` | the same, no older ref; every invariant but I8 and I1 | — | RED `INV_REF_BY_OBSERVATION_HAS_BODY` | 23,140 | 19 |
| `res_r1_token_recurs_by_observation_all_bodies` | the same, option | — | RED `INV_REF_BY_OBSERVATION_HAS_BODY` | 23,216 | 19 |
| `res_r1_token_recurs_pending_by_observation` | the same, a pending row; the qualified I8 alone | — | RED `INV_REF_BY_OBSERVATION_HAS_BODY` | 15,205 | 21 |
| `res_r3_inherited_ref` | R3 | an older ref on a token under a late delete, marker `Condemned` | RED `INV_COMMITTED_REF_HAS_BODY` | 90 | 11 |
| `res_r3_inherited_ref_all_bodies` | the same, option | — | RED `INV_COMMITTED_REF_HAS_BODY` | 99 | 12 |
| `res_r3_by_observation` | R3, refs of this build | — | green | 23,028 | 45 |
| `res_r3_by_observation_all_bodies` | the same, option | — | green | 23,028 | 45 |
| `res_r3_clean_marker_by_observation` | R3 with a `Clean` marker | — | RED `INV_REF_BY_OBSERVATION_HAS_BODY` | 1,111 | 22 |
| `res_r3_clean_marker_by_observation_all_bodies` | the same, option | — | green | 23,028 | 45 |
| `res_r4_started_in_window` | R4 | a member reads `_pool_meta` before M8 and claims after M7 | RED `INV_COMMITTED_REF_HAS_BODY` | 1,564,701 | 27 |
| `res_r2_resumed_in_mount_sequence` | R2 row 3 | a member suspended after its `_pool_meta` read | RED `INV_COMMITTED_REF_HAS_BODY` | 1,576,522 | 27 |
| `res_r2_suspended_writer_stood_clock` | R2 row 1 | a mounted member suspended; its clock stood still | RED `INV_COMMITTED_REF_HAS_BODY` | 2,208,474 | 26 |
| `res_floor1_recur` | R1 (e) at floor 1 | every late kind, case (e); `INV_LOSS_IS_RECURRED_TOKEN` with I5 and the forced option | cut at 60,619,702 states, no violation | 60,619,702 | — |
| `res_floor1_recur_crash` | R1 (e) at floor 1 | the same, 1 crash, 2 runs, faults | cut at 96,383,779 states, no violation | 96,383,779 | — |
| `res_r1_recur_loss_is_r1e` | R1 (e) as widened | late delete and `PUT`; `INV_LOSS_IS_R1E` alone: [trace](#r1-recur-g4) | RED `INV_LOSS_IS_R1E` | 113,972 | 22 |
| `res_floor1_recur_i8` | R1 (e) at floor 1 | late delete and `PUT`, case (e); I8 alone | RED `INV_COMMITTED_REF_HAS_BODY` | 1,291 | 3 |
| `res_r1_recur_loss_is_recurred_token` | case (e) at floor 3 | late delete and `PUT`; `INV_LOSS_IS_RECURRED_TOKEN` alone: every loss removed the recurring token | green | 5,619,671 | 51 |
| `res_r1_recur_i4` | R1 (e) as widened | late delete and `PUT`, no older ref; I5, I4 and the order invariant | RED `INV_I4_INTENT_PERMANENT` | 64,773 | 21 |
| `res_r1_recur_no_late` | none | case (e) seeds, `Late = {}`; every invariant | green | 120,574 | 48 |
| `res_floor1_r3` | R3 at floor 1 | an older ref on a token under a late delete; I8 alone | RED `INV_COMMITTED_REF_HAS_BODY` | 263 | 2 |
| `res_floor1_r3_by_observation` | R3 at floor 1, refs of this build | the same; every invariant but I8 | green | 586,554 | 48 |

### Sabotages of the tool {#sabotages}

Each removes one rule. "All" runs every invariant of the green rows and names the first that falls; "I8" runs
`INV_COMMITTED_REF_HAS_BODY` alone.

| Configuration | Rule removed | Scenario | All | I8 |
|---|---|---|---|---|
| `sab_mig_floor_before_intents` | S12: M5 before M8 | late delete, `Condemned` | RED `INV_COMMITTED_REF_HAS_BODY` (1,755; 14) | RED `INV_COMMITTED_REF_HAS_BODY` (1,828; 14) |
| `sab_mig_rows_only` | M5 for markers | spared row, `Condemned`, late delete | RED `INV_COMMITTED_REF_HAS_BODY` (1,025; 24) | RED `INV_COMMITTED_REF_HAS_BODY` (915; 22) |
| `sab_mig_unconfirmed_create` | S13: M5 confirm-or-fail | pending row, faults | RED `INV_DELETE_EVIDENCE` (1,967; 17) | RED `INV_COMMITTED_REF_HAS_BODY` (8,596; 20) |
| `sab_mig_gc_state_not_rewritten` | M6 | GC leader suspended in a round | RED `INV_COMMITTED_REF_HAS_BODY` (3,980,136; 29) | RED `INV_COMMITTED_REF_HAS_BODY` (4,316,641; 29) |
| `sab_mig_floor_before_gc_state` | M6 before M8 | the same | RED `INV_COMMITTED_REF_HAS_BODY` (3,866,738; 26) | RED `INV_COMMITTED_REF_HAS_BODY` (3,645,555; 26) |
| `sab_mig_no_recheck` | M7 | a member starts; claim before M7 | RED `INV_NO_LIVE_OLD_SLOT_AFTER_FLOOR` (42,808; 15) | RED `INV_COMMITTED_REF_HAS_BODY` (1,841,716; 27) |
| `sab_mig_markers_before_floor` | Q8: M9 after M8 | late delete, a crash, a rerun | RED `INV_TOOL_DESTROYS_NOTHING_BEFORE_FLOOR` (3,129; 13) | **green** (634,243; 66; 2s) |
| `sab_mig_raise_before_m5` | M6: the raise after M5 | floor 1; the raise right after M1; a crash, a rerun; late delete | RED `INV_FORCED_OPTION_COVERS_SEED_BODY` (13,404; 17) | RED `INV_REF_BY_OBSERVATION_HAS_BODY` (60,416; 22) |
| `sab_mig_no_row_token_intent` | M5: the pending row's own token | case (e), late delete and `PUT`, no older ref | RED `INV_DELETE_EVIDENCE` (13,774; 17) | — |

**`markers_before_floor` is green on I8: the rule is redundant for I8 in this model, not hidden by the bounds.** M9
runs after M5 in every order, so every marker it deletes was already turned into an intent, and intents persist: no
actor deletes one before the floor. A rerun after the crash finds no marker but finds the intent. The rule still
keeps an older build able to use the pool after a crash before M6 (§7.2, first row); the model does not run an older
build after a crash.

### Witnesses {#witnesses}

Each is an invariant that must fail; all fail.

| Witness | Branch shown | Distinct states | Depth |
|---|---|---|---|
| `wit_migrated` | the floor is raised | 496 | 13 |
| `wit_m2_refuses` | a late lease write moves the pair between the two M2 reads | 672 | 6 |
| `wit_m2_fences` | M2 fences a silent slot | 884 | 5 |
| `wit_m7_refuses` | a member claims its slot after M6; M7 refuses | 201,737 | 14 |
| `wit_m6_conflict_restarts` | a late `gc/state` commit lands between M3 and M6; the run restarts at M2 | 2,040 | 12 |
| `wit_rerun_skips_m6` | a rerun after a crash finds header 4 and skips M6 | 13,287 | 22 |
| `wit_crash_then_complete` | a crash, then a rerun completes | 1,915 | 16 |
| `wit_inherited_pending_executed` | this build deletes a row read pending before the tool, on M5's intent | 334 | 19 |
| `wit_inherited_candidate_confirms` | G3 on an inherited candidate meets M5's intent | 671 | 18 |
| `wit_adopt_inherited_body` | the writer adopts an inherited body | 1,380 | 18 |
| `wit_meets_tool_intent` | the writer meets M5's intent and publishes | 2,045 | 19 |
| `wit_marker_only_intent` | a `Condemned` marker with no row yields the intent | 292 | 28 |
| `wit_late_delete_misses` | a late delete misses after a publication of this build | 2,678 | 17 |
| `wit_m9_leaves_changed_marker` | a marker changed after M4 survives M9 | 1,755 | 15 |
| `wit_old_gc_round` | a resumed older GC leader commits a round | 607 | 4 |
| `wit_option_intent_on_clean` | the option gives an intent to a body with a `Clean` marker | 921 | 17 |
| `wit_two_runs_done` | two runs at once both complete | 3,654 | 17 |
| `wit_row_token_intent` | M5 creates the intent of a pending row's own token over an absent or other body | 415 | 11 |
| `wit_rerun_after_raise` | at floor 1, a crash after the raise; the rerun reads floor 3, runs without the option and completes | 4,236 | 18 |

## Reduced traces {#traces}

Tool steps M1 to M7 are omitted where nothing else happens between them.

### R1, a late `Clean` (`res_r1_late_clean`) {#r1-late-clean}

1. Seed: body `t1`; marker `Condemned`; no row (spared); an older `DELETE If-Match t1` in flight; a `Clean` swap in
   flight on the current marker version (case (b)).
2. The swap lands: marker `Clean`.
3. M1 to M7: M4 reads `Clean`, no row: M5 creates nothing.
4. M8: floor 4.
5. Writer: W2 sees `t1`, W4 reads no intent, W5 sees `t1`: adopts.
6. The older delete lands: `t1` removed.
7. The writer commits: a ref with no body.

`res_r1_marker_anomaly` is the same from step 3 with the marker already `absent` or `Clean`.
`res_r1_missed_listing` is the same with the marker `Condemned` and M4's listing omitting the key; with the option
the body is omitted too, so it stays red.

### R1, a recurring token (`res_r1_token_recurs`) {#r1-recur}

1. Seed: body `t1`, marker `Clean` (the older writer published `t1` and wrote it); an older ref on `B`; an older
   `DELETE If-Match t2` in flight for the replaced token `t2`; a second copy of `t2`'s staging object in flight.
2. The copy lands: `t2` is current again (A1 broken in the older build).
3. The delete lands: the older ref names no body.

With the option M5 makes `I(B,t1)`, which says nothing about `t2`: still red. On refs of this build
(`res_r1_token_recurs_pending_by_observation`): a writer of this build publishes `t3`, the copy lands `t2` over it,
the delete removes `t2`, and the writer's committed ref names no body.

### Case (e), G4 of this build (`res_r1_recur_loss_is_r1e`) {#r1-recur-g4}

1. Seed: no body at `B`; row `pending(t2)`; a second copy of `t2`'s staging object in flight. No older delete is
   needed.
2. M5: `HEAD B` absent; M5 creates and confirms `I(B,t2)` for the row's own token. M6 to M8.
3. GC of this build: lease read and cut; in-degree 0.
4. Writer of this build: W1 after that cut; W2 absent; W5 publishes `t3`, answered.
5. The delayed copy lands: `B` is `t2` again, over `t3`.
6. The writer commits its ref.
7. The round's G4: `HEAD` shows `t2`, the row's token; `DELETE If-Match t2` removes it. The ref names no body.

I5 holds: the intent was confirmed before the cut. Precondition: the older copy is still in flight after M8. R1 (e)
as widened names it.

**Case (e) and I4** (`res_r1_recur_i4`): row `pending(t2)`, no body. M5 confirms `I(B,t2)`. G4 finds no body and
commits `standing(t2)`. The delayed copy lands `t2`. The next round's G8 deletes `I(B,t2)` with no `HEAD`, while
`t2` is current. A writer can then adopt `t2`, and a delayed older delete of `t2` removes it: the loss R1 (e) names.

### R3, an inherited reference {#r3}

1. Seed: body `t1`; marker `Condemned`; no row (spared by the ref); an older ref on `B`, adopted under an anomaly; an
   older `DELETE If-Match t1` in flight.
2. The delete lands, before or after the tool: the older ref names no body. The linker may copy it first; the copy
   inherits the exposure.

M5 makes `I(B,t1)` from the marker, so a writer of this build meets the intent and publishes `t'`; the delete then
misses. `res_r3_by_observation` is green with and without the option.

With a `Clean` marker in place of `Condemned` (`res_r3_clean_marker_by_observation`), M5 makes no intent without the
option and the writer adopts `t1` (R1, the trace above). With the option it makes `I(B,t1)`, and the qualified I8 is
green again.

### R4, a member started in M7's window (`res_r4_started_in_window`) {#r4}

1. Seed: body `t1`, marker `Clean`.
2. M1 to M7: no slot moved.
3. The member reads `_pool_meta` (floor 3) and claims its slot.
4. M8: floor 4. The member never reads `_pool_meta` again.
5. GC of this build: G2 condemns `t1`; next round G3 creates `I(B,t1)` and commits pending; next round takes its cut
   at in-degree 0.
6. The older writer adds its edge, `HEAD` sees `t1`, reads the `Clean` marker (M9 has not run): adopts.
7. G4: `HEAD` sees `t1`, `DELETE`. The older writer commits: a ref with no body.

`res_r2_resumed_in_mount_sequence` is the same with the `_pool_meta` read before the tool starts and the claim after
M7. `res_r2_suspended_writer_stood_clock`: the member is mounted and suspended; M2 fences its slot; it resumes after
M7 with a lease its stopped clock still counts valid, and runs steps 5 to 7 before its next renewal is refused.

### Sabotages {#sabotage-traces}

- `sab_mig_raise_before_m5_i8`: floor 1; seed `t1`, marker `absent`, no row, an older `DELETE If-Match t1` in
  flight. M1, then the raise to floor 3, then a crash. The rerun reads floor 3: no forced option. M5 makes no intent
  (no marker that is not `Clean`, no row). M8. A writer adopts `t1`; the delete lands.
- `sab_mig_floor_before_intents`: seed `t1`, `Condemned`, no row, a late delete. M8 runs after M4. The writer adopts
  `t1` before M5; the delete lands. S12.
- `sab_mig_unconfirmed_create`: seed pending row on `t1`. M5's create is unanswered, never applied, and accepted. M8.
  GC of this build reads the row pending and deletes `t1` with no intent (`INV_DELETE_EVIDENCE`). With a late delete
  also in flight, a writer adopts on the missing intent and the late delete lands (I8). S13.
- `sab_mig_gc_state_not_rewritten`: a GC leader suspended in a round resumes just before M8. `gc/state` is still
  header 3, so its round commits and its next lease reads succeed: it condemns `t1` (marker `Condemned`), graduates
  it on that marker, then, in a third round, takes its cut. A writer of this build adds its edge after that cut and adopts `t1` (it does not read markers);
  the older GC sends `DELETE If-Match t1`; it lands.
- `sab_mig_floor_before_gc_state`: the same, with the resumed rounds between M8 and M6.
- `sab_mig_no_recheck`: a member claims its slot after M2; M7 does not read; the pool is migrated under a live older
  member, then the R4 trace from step 5.

## Specification corrections {#corrections}

- **R1 (e) widened.** One delayed token-repeating copy of an older writer is enough; the deleter can be G4 of this
  build on a confirmed intent ([trace](#r1-recur-g4)), or G8 removing `I(B,t)` while a restored `t` is current.
  §7.4 at `d16ec5f5eec` names only an older GC's delete.
- The Q8 rule is not needed for I8 (see [Sabotages](#sabotages)); its reason is the restart table's first row.
- R3 and R1 coincide when the older anomaly left a `Clean` marker; the option protects new adoptions of that token,
  not the existing ref. §7.1 says the option "does not cover a ref that already exists (R3)"; the model agrees.

## Bounds, and what the model does not prove {#bounds}

- **One blob key, one older root.** No catalog of many roots, no namespaces; the listing of `gc/server-roots/` and
  the catalog reads are one slot.
- **The older world is seeded, not run.** `Consistent` encodes the argument of §7.4; its proof belongs to the marker
  protocol's models (`CaBlobPublishCore.tla`, `CaGcCondemnMarkerGate.tla`). An older member that starts or resumes
  runs a reduced marker writer and marker GC (no marker delete).
- **Small numbers.** One publication per build; at most one late request of each kind; 4 to 6 `gc/state` writes;
  2 crashes; two runs at once only with one crash. A member renews at most twice per claim.
- **This build is reduced.** Its publications and GC requests are answered; their faults, `cas-fsck` and the rebuild
  are in `CaBlobIntentProtocol`.
- **M2 is one re-read.** A slot silent through it means the member's lease expired on its own clock, unless the clock
  stood still: the observer lease of M2 is the member's `mount_lease_ttl_ms`.
- **Not modelled:** the probe, the dialect, the prefix length, `--dry-run`, malformed markers, tokens that do not
  canonicalise, a `gc/state` missing at M3, the grace interval as time.
- **Not run on this model:** `mig_two_runs` (two tools at once), `live_migration_completes` and `mig_all_late`; their
  earlier forms took 841 s, 1226 s and over 989 s. `live_floor1_completes` stands in for liveness;
  `mig_all_late_no_crash` and the per-kind rows for `mig_all_late`.
- **G9 is not modelled.** An orphan intent stays; it only blocks adoption.
