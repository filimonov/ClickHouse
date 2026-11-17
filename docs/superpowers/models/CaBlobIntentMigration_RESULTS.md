---
description: 'TLC evidence for the offline migration of a CAS pool to blob deletion intents (clickhouse-disks cas-migrate-pool, M1 to M9): green configurations, admitted residuals R1 to R4 shown red, sabotages of the tool, witnesses, liveness, bounds.'
sidebar_label: 'CAS blob intents migration TLC results'
sidebar_position: 6
slug: /superpowers/models/CaBlobIntentMigration-results
title: 'CAS blob intents migration TLC results'
doc_type: 'reference'
---

# `CaBlobIntentMigration` results {#cablobintentmigration-results}

Model of §7 of `docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md` at commit `b2c6126f7ee`.
Step and rule ids (M1 to M9, W2 to W6, G1 to G8, I4 to I8, R1 to R4, S12, S13) are the specification's.
`CaBlobIntentMigration_<name>.cfg` holds configuration `<name>`.

## Verdict {#verdict}

**The migration as specified passes. No rule had to change; one residual reaches further than the text says.**

- **Green.** With every older member stopped: every seed the argument of §7.4 admits, each kind of late older
  request, a crash after every step with reruns, two runs at once, the option on and off. A suspended GC leader and
  a suspended mounted writer whose clock kept running are green too. A member that starts before the M7 read is
  detected: M7 refuses. With no older member, a run that is not refused reaches `migration complete` (liveness).
- **Admitted residuals are red when allowed.** R1 cases (a) to (e), R3, R4 and R2 (resumed inside the mount
  sequence; and a mounted writer whose clock stood still) each produce a committed ref with no body.
- **Sabotages.** Six of seven are red on I8 or on `INV_DELETE_EVIDENCE`. `markers_before_floor` (M9 before M8) is red
  only on the invariant that restates the rule; I8 stays green, see [Sabotages](#sabotages).
- **Case (e) also defeats I5.** A recurring token lets this build's first round delete an inherited pending row's
  token with no confirmed intent. It is inside residual R1 (A1 broken), but G8 and I5 do not say so; see
  [Specification corrections](#corrections).
- **One point the specification does not say.** With a `Clean` marker over a token under an older delete, an
  inherited ref (R3) comes with an R1 exposure for refs this build adopts; `--intents-for-all-bodies` closes the
  second, not the first. See [R3](#r3).

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
| Seed (`Init`, `Consistent`) | The older world when the tool starts: body token 1 or no body; marker `absent`, `Clean`, `Condemned`; row none, candidate or pending on token 1 or on a replaced token 2; an older ref; late requests. `Consistent` is §7.4 "Which tokens a late delete can name": while `t` is current and under older delete authority (a pending row or a sent delete), the marker is `Condemned`, no marker request in flight can still change it, and no older writer adopted `t`. `Anomaly` drops one clause at a time |
| Late requests (§7.4) | GC `DELETE B If-Match t`; writer body `PUT` (fresh, or the recurring token under case (e)); writer `Clean` create or swap; GC `Condemned` write or marker delete; GC `gc/state` commit or lease write. Each lands at most once, at any time, or never |
| The tool (§7.1) | `TM1` to `TM9`, one action per step; M2 is two reads with the fence; M5 a send, an answer and an unanswered branch with the resolve read. `TCrash` after any step, `TStart` reruns from M1. `Tools` runs at once |
| This build (§3) | Runs only after M8 (§2.3). Writer W2, W4, W5, W6; linker (§4.2, no request); GC: lease read and cut, one fold step (G2, G3, G4, G6, G7), commit at header 4 |
| Older member (§7.4 R2, R4) | Mount sequence (`_pool_meta` read, slot claim), slot renewal, `gc/hb`, a marker writer and a marker GC. `OldMember` picks the scenario |

### Invariants {#invariants}

| Invariant | Statement |
|---|---|
| `INV_COMMITTED_REF_HAS_BODY` | I8 unqualified: any committed ref (older, this build's, a copy) names a present body |
| `INV_REF_BY_OBSERVATION_HAS_BODY` | I8 as qualified in §4.1: a ref this build commits by W2 to W6, or a copy of one |
| `INV_DELETE_EVIDENCE` | I5 with G8: a body `DELETE` of this build has a confirmed intent for its token (G3's on the row, or M5's), a committed pending row and a later cut at in-degree 0 |
| `INV_I4_INTENT_PERMANENT` | no intent `DELETE` while its token is current |
| `INV_TOOL_DESTROYS_NOTHING_BEFORE_FLOOR` | §7.1 "Order": the tool deletes nothing before M8 |
| `INV_NO_LIVE_OLD_SLOT_AFTER_FLOOR` | A8 as M2, M7 and the fence enforce it |
| `INV_I1_NO_RECURRENCE` | diagnostic; red only under case (e) |
| `LIVE_MIGRATION_COMPLETES` | a run reaches `migration complete` and the floor stays raised |

## Runs {#runs}

Defaults: one tool, no older member, every seed row, marker, body-absent and older-ref choice, no late request, no
anomaly, option off, no fault, linker on, no crash, one run, one publication per build, 6 `gc/state` writes.
Configurations with an older member use smaller seeds: no older ref, no linker, a body present, rows `none` or
`pend`, markers `Clean` or `Condemned`, 4 `gc/state` writes, two runs, no late request except for the GC leader.

### Green {#green}

| Configuration | Differs from the defaults | Result | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `mig_base` | — | green | 92,198 | 47 | 0s |
| `mig_base_all_bodies` | option | green | 94,806 | 47 | 0s |
| `mig_crash_after_each_step` | 2 crashes, 3 runs, faults, late delete | green | 4,459,910 | 66 | 13s |
| `mig_crash_after_each_step_all_bodies` | the same, option | green | 4,665,572 | 66 | 14s |
| `mig_two_runs` | two tools, 1 crash, 3 runs, late delete | green | 201,357,053 | 71 | 841s |
| `mig_late_old_delete` | late delete | green | 263,565 | 47 | 1s |
| `mig_late_old_delete_all_bodies` | the same, option | green | 268,781 | 47 | 1s |
| `mig_late_old_put` | late delete, late `PUT` | green | 2,002,480 | 48 | 6s |
| `mig_late_old_marker_write` | late delete, `Condemned` write, marker delete | green | 1,622,408 | 48 | 5s |
| `mig_late_old_clean` | late delete, `Clean` create, `Clean` swap | green | 1,300,850 | 48 | 4s |
| `mig_late_old_gc_commit` | late delete, `gc/state` commit, lease write; 3 runs | green | 1,782,839 | 56 | 5s |
| `mig_all_late_no_crash` | every late kind; 3 runs; body present; no linker | green | 40,877,234 | 55 | 117s |
| `mig_spared_row_marker_only` | row none, `Condemned`, late delete | green | 18,407 | 45 | 0s |
| `mig_inherited_pending_executed` | row pending, late delete | green | 91,708 | 47 | 0s |
| `mig_member_started_before_recheck` | a member starts; its claim precedes a passed M7 | green | 13,077,099 | 70 | 32s |
| `mig_suspended_gc_leader` | a GC leader suspended in a round, late delete | green | 19,323,866 | 63 | 55s |
| `mig_suspended_writer` | a mounted member suspended; its clock keeps running | green | 4,542,689 | 60 | 12s |
| `live_migration_completes` | `LiveSpec`; 2 crashes, 4 runs; late delete, `gc/state` commit and lease, `Clean` create | green | 33,681,133 | 70 | 1226s |

`mig_rebuilt_state_marker_only` of §10.1 is the same seed as `mig_spared_row_marker_only` here: a row lost in a
rebuild and a spared row both leave no row and a `Condemned` marker.

### Admitted failing histories (§10.1 (b)) {#residuals}

Each is run against `INV_COMMITTED_REF_HAS_BODY` alone (the `_by_observation` rows: every invariant but that one).

| Configuration | Residual | Switch | Result | Distinct states | Depth |
|---|---|---|---|---|---|
| `res_r1_marker_anomaly` | R1 (a) to (c) | marker `absent` or `Clean` over a token under a late delete | RED | 1,701 | 19 |
| `res_r1_marker_anomaly_all_bodies_by_observation` | the same, option | — | green | 36,814 | 45 |
| `res_r1_late_clean` | R1 (b) | a late `Clean` swap on the current marker version | RED | 3,482 | 19 |
| `res_r1_late_clean_all_bodies_by_observation` | the same, option | — | green | 91,981 | 46 |
| `res_r1_missed_listing` | R1 (d) | M4's listing omits the key | RED | 1,984 | 21 |
| `res_r1_missed_listing_all_bodies` | the same, option | — | RED | 1,446 | 19 |
| `res_r1_token_recurs` | R1 (e) | a late `PUT` lands the replaced token | RED | 1,106 | 3 |
| `res_r1_token_recurs_all_bodies` | the same, option | — | RED | 1,102 | 3 |
| `res_r1_token_recurs_by_observation` | the same, no older ref; every invariant but I8 and I1 | — | RED on `INV_DELETE_EVIDENCE` | 11,400 | 15 |
| `res_r1_token_recurs_by_observation_all_bodies` | the same, option | — | RED on `INV_DELETE_EVIDENCE` | 11,160 | 15 |
| `res_r1_token_recurs_pending_by_observation` | the same, a pending row; the qualified I8 alone | — | RED | 12,196 | 17 |
| `res_r3_inherited_ref` | R3 | an older ref on a token under a late delete, marker `Condemned` | RED | 84 | 10 |
| `res_r3_inherited_ref_all_bodies` | the same, option | — | RED | 110 | 12 |
| `res_r3_by_observation` | R3, refs of this build | — | green | 19,018 | 46 |
| `res_r3_by_observation_all_bodies` | the same, option | — | green | 19,018 | 45 |
| `res_r3_clean_marker_by_observation` | R3 with a `Clean` marker | — | RED on the qualified I8 | 1,241 | 20 |
| `res_r3_clean_marker_by_observation_all_bodies` | the same, option | — | green | 19,018 | 46 |
| `res_r4_started_in_window` | R4 | a member reads `_pool_meta` before M8 and claims after M7 | RED | 1,237,730 | 25 |
| `res_r2_resumed_in_mount_sequence` | R2 row 3 | a member suspended after its `_pool_meta` read | RED | 1,217,629 | 25 |
| `res_r2_suspended_writer_stood_clock` | R2 row 1 | a mounted member suspended; its clock stood still | RED | 1,323,396 | 24 |

### Sabotages of the tool {#sabotages}

Each removes one rule. "All" runs every invariant of the green rows and names the first that falls; "I8" runs
`INV_COMMITTED_REF_HAS_BODY` alone.

| Configuration | Rule removed | Scenario | All | I8 |
|---|---|---|---|---|
| `sab_mig_floor_before_intents` | S12: M5 before M8 | late delete, `Condemned` | RED `INV_COMMITTED_REF_HAS_BODY` (2,877; 14) | RED (3,619; 15) |
| `sab_mig_rows_only` | M5 for markers | spared row, `Condemned`, late delete | RED `INV_COMMITTED_REF_HAS_BODY` (1,037; 20) | RED (1,054; 21) |
| `sab_mig_unconfirmed_create` | S13: M5 confirm-or-fail | pending row, faults | RED `INV_DELETE_EVIDENCE` (1,503; 15) | RED (8,219; 19) |
| `sab_mig_gc_state_not_rewritten` | M6 | GC leader suspended in a round | RED `INV_COMMITTED_REF_HAS_BODY` (3,377,232; 28) | RED (3,351,015; 28) |
| `sab_mig_floor_before_gc_state` | M6 before M8 | the same | RED `INV_COMMITTED_REF_HAS_BODY` (5,167,234; 26) | RED (5,794,993; 26) |
| `sab_mig_no_recheck` | M7 | a member starts; claim before M7 | RED `INV_NO_LIVE_OLD_SLOT_AFTER_FLOOR` (20,111; 13) | RED (1,450,707; 25) |
| `sab_mig_markers_before_floor` | Q8: M9 after M8 | late delete, a crash, a rerun | RED `INV_TOOL_DESTROYS_NOTHING_BEFORE_FLOOR` (2,187; 11) | **green** (508,960; 60; 2s) |

**`markers_before_floor` is green on I8: the rule is redundant for I8 in this model, not hidden by the bounds.** M9
runs after M5 in every order, so every marker it deletes was already turned into an intent, and intents persist: no
actor deletes one before the floor. A rerun after the crash finds no marker but finds the intent. The rule still
keeps an older build able to use the pool after a crash before M6 (§7.2, first row); the model does not run an older
build after a crash.

### Witnesses {#witnesses}

Each is an invariant that must fail; all fail.

| Witness | Branch shown | Distinct states | Depth |
|---|---|---|---|
| `wit_migrated` | the floor is raised | 418 | 11 |
| `wit_m2_refuses` | a late lease write moves the pair between the two M2 reads | 674 | 6 |
| `wit_m2_fences` | M2 fences a silent slot | 839 | 5 |
| `wit_m7_refuses` | a member claims its slot after M6; M7 refuses | 78,112 | 12 |
| `wit_m6_conflict_restarts` | a late `gc/state` commit lands between M3 and M6; the run restarts at M2 | 1,589 | 10 |
| `wit_rerun_skips_m6` | a rerun after a crash finds header 4 and skips M6 | 7,548 | 18 |
| `wit_crash_then_complete` | a crash, then a rerun completes | 1,567 | 14 |
| `wit_inherited_pending_executed` | this build deletes a row read pending before the tool, on M5's intent | 265 | 15 |
| `wit_inherited_candidate_confirms` | G3 on an inherited candidate meets M5's intent | 631 | 16 |
| `wit_adopt_inherited_body` | the writer adopts an inherited body | 1,906 | 17 |
| `wit_meets_tool_intent` | the writer meets M5's intent and publishes | 2,190 | 17 |
| `wit_marker_only_intent` | a `Condemned` marker with no row yields the intent | 222 | 24 |
| `wit_late_delete_misses` | a late delete misses after a publication of this build | 2,733 | 15 |
| `wit_m9_leaves_changed_marker` | a marker changed after M4 survives M9 | 1,535 | 13 |
| `wit_old_gc_round` | a resumed older GC leader commits a round | 602 | 4 |
| `wit_option_intent_on_clean` | the option gives an intent to a body with a `Clean` marker | 976 | 15 |
| `wit_two_runs_done` | two runs at once both complete | 3,217 | 15 |

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

**Case (e) also breaks I5 for this build's own GC** (`res_r1_token_recurs_by_observation`):

1. Seed: no body; row pending on `t2`; marker `absent`; an older `DELETE If-Match t2` and a second copy of `t2` in
   flight.
2. M5: the pending row asks for `HEAD B`: absent, so no intent.
3. The copy lands: `t2` is current.
4. M6 to M8.
5. GC of this build reads the row pending, takes its cut at in-degree 0, `HEAD` sees `t2`: `DELETE` with no intent
   ever confirmed for `t2`.

The model did not isolate a loss from this delete alone; the older delete reaches the loss first.

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

None needed for safety outside the admitted residuals. Three notes for §7 and §10.1:

- **Case (e) reaches this build's own deletes.** G8 and I5 say an inherited pending row's intent is M5's "for the
  token current at that read". If the row's token is absent at M5 and the recurring copy lands after it, the first
  round of this build deletes that token with no intent. Smallest text correction: R1 (e) says that this build's
  first round may then delete too. Smallest protocol correction, if wanted: M5 creates `I(B, row token)` for every
  pending row whose body is absent or carries another token (one `PUT` per such row).

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
- **Not finished:** `mig_all_late` (every late kind with crashes and faults) was stopped at 325,266,871 distinct
  states, depth 35, 989 s, no violation; `mig_all_late_no_crash` and the per-kind rows stand in for it.
