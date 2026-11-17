---
description: 'TLC evidence for CAS blob publication without a birth marker: deletion intents, write-ahead GC, cas-fsck cleanup, the transition from the marker protocol and the pool created by the new build. Green configurations, sabotages with reduced traces, witnesses, liveness, bounds.'
sidebar_label: 'CAS blob intents TLC results'
sidebar_position: 5
slug: /superpowers/models/CaBlobIntentProtocol-results
title: 'CAS blob deletion intents TLC results'
doc_type: 'reference'
---

# `CaBlobIntentProtocol` and `CaBlobIntentFreshPool` results {#cablobintentprotocol-results}

## What still holds after the design dropped the online transition {#scope-after-offline-migration}

The design moves an older pool once, offline, with `clickhouse-disks cas-migrate-pool`. Its model is
`CaBlobIntentMigration.tla`, results in `CaBlobIntentMigration_RESULTS.md`.

- **Valid: the steady state.** Every configuration with `Start = "T3"`, `Builds = {"new"}`, `OldGCs = {}`, a seed
  that is not `legacy*`, and no transition sabotage. With these constants every body is marked and every writer has
  learned, so the module runs the protocol of the current specification. These rows, and their `.cfg` files, stand.
- **Record only: the online transition and the fresh pool.** Rows with `Start = "T1"`, a `legacy*` seed, an older
  writer build, an older GC, or one of the sabotages `adopt_unmarked`, `mark_before_learned`, `no_stamp`,
  `old_gc_after_stamp`, `lost_stamp_old_bootstrap`, `mode_cached`, `mode_on_failed_read`, `old_writer_after_switch`,
  `inherited_executed`; and every `fp_*` row. Their `.cfg` files and `CaBlobIntentFreshPool.tla` are removed. The
  tables below keep their numbers as a record of the dropped design.
- **The module is unchanged.** Removing the transition from it (`Stamp`, `Switch`, `LoseStamp`, `GMode`, `WLearn`,
  the mark bit, the older builds) would be a rewrite. Under the steady-state constants those actions are disabled.
- **Checked after the removal:** `safe_base` re-run gives 3,639,570 distinct states and `safe_late_put` 1,875,003,
  equal to the table [Green, steady state](#green-steady-state).

Models of `docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md` as it stands at commit
`d2f130ebfef`. Section numbers and rule ids (W1, G3, F2, I5, A8, S1, X1) are the specification's.

- `CaBlobIntentProtocol.tla`: one blob key; writers of the new and of the older build; GC actors of both builds that
  race on one `gc/state`; `cas-fsck`; the stamp and the switch. Steady state and transition are one module.
- `CaBlobIntentFreshPool.tla`: §7.4, a pool created by the new build (states N0 to N4).
- `CaBlobIntentProtocol_<name>.cfg` holds configuration `<name>`; `CaBlobIntentFreshPool_<name>.cfg` holds the
  configuration that the tables call `fp_<name>`.

## Verdict {#verdict}

**The rules of the specification pass. No rule had to change and none was added.**

- **Green.** Every configuration that ran to completion is green: the steady state on both token dialects with each
  late-landing kind, two GC actors, lost commits, `cas-fsck` cleanup, publication integrity and the request budget;
  the transition from every seed with older and new writers and older and new GC; the pool created by the new build.
  The one row that mixes everything, `safe_transition` (both writer builds with GC of both builds), did not finish in
  its time limit: 109 million states, no violation; its halves are green. [Cut or not finished](#cut) lists what
  did not finish; none showed a violation before it was stopped.
- **Red.** Every sabotage produces a counterexample on a named invariant, except one. 23 of the 26 switches reach I8
  (a committed ref with no body). `sab_unchecked_stream` falls on I2 only, as intended. `sab_inherited_executed` (G8
  removed) falls on I5 only: I8 holds without G8. `sab_candidate_from_intent` (G9 removed) stays green on every
  invariant.
- **Witnesses.** All 28 reachability witnesses of the protocol module and all 8 of the fresh-pool module fire.
- **Liveness.** With no GC faults and `cas-fsck` running, no row is stuck and no intent of a dead token is left. A
  pool created by the new build reaches T3. Shown red on purpose, because the design does not claim them: every
  unreferenced body is deleted; stale intents disappear without `cas-fsck`.

## Differences from §10.1 of the specification {#differences}

§10.1 of the specification at commit `d2f130ebfef` lists the expected verdict of each row. Four points differ.

| Where | The specification expects | The model shows |
|---|---|---|
| Row `sab_candidate_from_intent`; §3.5; G9 | red | green, 2,066,296 distinct states, depth 59, 15s; with late intent create and delete and `cas-fsck`: green, 40,226,218 distinct states, depth 80, 386s. `wit_derived_row_deletes` shows the derived row reaches its body `DELETE`. A row derived from an intent that is read over the current body is the equal-occupant confirmation of G3; the delete still needs the commit, a later cut and the `HEAD`. G9's ban is not a safety rule in this model |
| Row `sab_inherited_executed`; §7.6 | red on I8 or on T3 (a) | red on `INV_I5_DELETE_EVIDENCE` (T3 (c)) only. I8 alone: green, 63,900,752 distinct states, depth 74, 439s; with older writers, late `put` and the linker: green, 3,545,494 distinct states, depth 68, 14s. An inherited row names a pre-switch, unmarked token, which W3 never adopts. G8 keeps T3 (c) true and covers the unsupported cases X3 and X4 |
| §7.8, X1: "adopts a marked body on a stale `Clean`" | the exposure is a marked body under a stale marker | the shortest loss trace of `sab_switch_with_old_writer` is an older writer that adopts a legacy body on its own valid `Clean`, while intent-mode GC, which writes no `Condemned`, deletes it. Every key with a decodable `Clean` is exposed |
| Module and row names | `CaBlobIntentCore`, `CaBlobIntentTransition`; rows `safe_transition`, `safe_late_old_requests` | one protocol module and one fresh-pool module; the two rows are split, see [Cut or not finished](#cut) |

Three more facts the runs establish:

- **A8 as written is enough.** A writer of the older build whose edge was durable before the switch may go on and
  commit after it: every transition row is green with that allowed. Only an older writer that starts after the switch
  is red. Step 3 of §7.2 is stricter than the model needs.
- **G7 names only "another identity".** For a touched row whose `HEAD` shows no body, or the same identity, the model
  follows `CasBlobInDegree.cpp:612-631`: the row is settled by G3, G4 or G6.
- **The alternative of §4.3 is safe in the model.** `alt_adopt_changed_token`: green, 1,856,661 distinct states, depth 59, 14s;
  `wit_alt_adopt_changed` shows the branch is taken and commits.

## Checker and how to run {#checker}

- TLC `2026.07.18.145032`, revision `30cc360`; `tla2tools.jar` SHA-256
  `cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3`; OpenJDK 21.0.9; 8 workers.
- One configuration:

```bash
java -Xmx16g -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -noGenerateSpecTE -workers 8 \
    -metadir <dir under tmp> -config CaBlobIntentProtocol_safe_base.cfg CaBlobIntentProtocol.tla
```

- `-noGenerateSpecTE` keeps TLC from writing trace modules next to the sources. Deadlock checking is off in every
  configuration: a quiet system is a legal end state.
- Counts of complete runs do not depend on the worker count. Counts of runs that end in a violation do.
- Liveness rows need steps that the safety rows do not: the condemn trigger of the code, unbounded rounds, fairness,
  and a round that ends by itself when its mode read or its commit fails. With `Trigger=any` and `Faults=TRUE` these
  do not change the reachable states: 51 complete rows were run on an earlier and on the final revision, and all 51 counts are equal (for example `safe_base` 3,639,570; `safe_refused_create` 5,001,216; `safe_inherited_demoted_1gc` 2,801,026; `sab_candidate_from_intent` 2,066,296; `live_late` 3,434,396).
- Eight long rows were not repeated after those steps were added and carry the counts of the run before:
  `safe_cleanup_spared` and `safe_integrity` (both dialects), `safe_all_late`, `safe_inherited_demoted`,
  `sab_inherited_executed_L`, and the three sabotages with the older GC.

## What the model separates {#granularity}

- **A read** (`HEAD`, `GET`) is one atomic observation taken between its send and its answer; the answer is used
  arbitrarily later.
- **A write** is three steps: send, apply, respond. A sender may stop waiting at any moment. With the kind in `Late`
  the request stays in flight and may apply later; otherwise it is never applied. Kinds: `put` body publication,
  `ic` intent create, `id` intent delete, `bd` body delete.
- **The create of G3** has its own resolve read: a refused create, or an unanswered one, is settled by a `GET` of the
  intent, which may itself fail.
- **A GC round** is lease read, mode read (G1), cut, fold and delete batch, one commit by compare-and-swap. Deletes go
  out before the commit. Two actors may run rounds at once; the one whose commit loses is the deposed leader.
- **`cas-fsck`** is two observations (intent `GET`, then `HEAD B`) and a delete nobody waits for.
- **The transition** has the stamp and the switch as separate steps, a learned flag per writer slot, the mark bit
  decided at staging, GC mode sampled per round, an older GC that the stamp stops, an older writer that adopts on a
  `Clean` marker.

## Rule of the specification to model action {#mapping}

| Spec rule | Model action (module `CaBlobIntentProtocol` unless named) | Store requests it stands for |
|---|---|---|
| W1 edge before the first request; §3.1 mark bit at staging | `WPrecommit` | precommit write (not a blob request) |
| §3.1 learning (read `_pool_meta`, dialect, probe) | `WLearn` | `GET _pool_meta` |
| W2 first `HEAD`; W3 unmarked or not learned | `WHead1` | `HEAD B` |
| W4 intent `GET` | `WGetIntent` | `GET I(B,t)` |
| W5 second `HEAD`: adopt, or publish once | `WHead2` | `HEAD B` |
| W6 publication: send | `WSendPut` (streamed, or the verbatim staged copy) | `PutObject` / multipart / `CopyObject` |
| W6 publication: apply | `PutLand` | — |
| W6 answered success, W7 nothing follows | `WPutOk` | — |
| W6 unanswered: next loop iteration from W2 | `WPutGiveUp` | — |
| W6 answered failure, refused read, digest mismatch (§6.3), writer death | `WAbandon` | — |
| I3 commit under the live precommit | `WCommit` | ref commit |
| part drop | `WDrop` | — |
| §4.2 no-bytes reference (`adoptEvidence`) | `WLink` | none |
| Older writer: marker `GET` after `HEAD`, adopt on `Clean`; `Clean` write after its `PUT` (§2.4) | `WGetMetaOld`, `WPutOk` | `GET .meta`; `PUT .meta` |
| A1 broken: transport reissue | `PutResend` (`MaxResends > 0`) | a second apply of one request |
| lease read of `gc/state`; older GC refused by header 4 (§2.4) | `GStart` | `gc/state` read-modify-write |
| G1 mode per round, after the lease | `GMode` | `GET _pool_meta` |
| the cut (A7) | `GCut` | — |
| G2 condemn (intent mode: `HEAD` only; marker mode: plus the `Condemned` write) | `GCondemn` | `HEAD B` (+ marker `GET`, `PUT`) |
| G6 spare, intent stays (G9) | `GSpare` | none |
| G7 supersede, best-effort delete of the old intent | `GSupersede` | `HEAD B`, intent `DELETE` |
| G3 create: send | `GCreateSend` | `PUT I(B,t) If-None-Match: *` |
| G3 create: apply (created or refused) | `ReqApply` on `ic` | — |
| G3 create: answered (`Committed`, or `Conflict` → read) | `GCreateAnswer` | — |
| G3 create: unanswered | `GCreateUnanswered` | — |
| G3 resolve read: equal occupant, `ProvenAbsent`, `NotObserved` | `GCreateResolve` | `GET I(B,t)` |
| G8 demotion of an inherited pending row | `Demoted`, used by `GCreateSend`, `GDelHead` | — |
| marker-mode graduation gate (today) | `GGraduateMarker` | marker `GET` (+ `PUT`) |
| G4 (1), (2) `HEAD`, body delete send; `Gone` and `Mismatch` without a request | `GDelHead` | `HEAD B`, `DELETE B If-Match` |
| G4 (2) apply | `ReqApply` on `bd` | — |
| G4 (2) answered, (3) intent delete send, (4) legacy marker | `GDelAnswer` | intent `DELETE` (+ marker `HEAD`, `DELETE`) |
| G5 unanswered body delete fails the round | `GDelUnanswered` | — |
| G4 (3) apply | `ReqApply` on `id` | — |
| G4 (3) answered, (5) row leaves | `GIdAnswer` | — |
| G5 unanswered intent delete fails the round | `GIdUnanswered` | — |
| round commit (compare-and-swap on the body the lease read); a lost compare-and-swap | `GCommit`, `GCommitLost` | `gc/state` conditional write |
| leader death, lost lease, failed read (G1, G2, G7) | `GAbort`; `GMode` for a mode the round cannot run in | — |
| F1, F2 first read (intent `GET`) | `FsckGetIntent` | `LIST blob-intents/`, `GET I(B,t)` |
| F2 second read (`HEAD B`), F3 delete; F0 admission | `FsckHeadBody`, `FsckDelete` | `HEAD B`, intent `DELETE` |
| §7.3 step 2, the stamp | `Stamp` | `gc/state` read-modify-write |
| §7.3 step 3, the switch | `Switch` | `_pool_meta` read-modify-write |
| §7.4 N0, N1 | `CaBlobIntentFreshPool`: `MRead`, `MCreateCatalog`, `MCreateMeta` | catalog and `_pool_meta` creates |
| §7.4 N2 stamp, N3 switch (send and apply), N4 learn | `CaBlobIntentFreshPool`: `MDecide`, `MStampSend`, `MSwitchSend`, `WriteApply`, `MLearn` | the two read-modify-writes |
| §7.4 crash and restart, concurrent creators, older build racing | `CaBlobIntentFreshPool`: `Crash`, two mounts, `OldCreate` | — |


## Invariants {#invariants}

The number in a name is the number of the specification.

| Specification | TLA+ invariant | States what |
|---|---|---|
| I1 | `INV_I1_NO_RECURRENCE` | a token that left `B` never lands on it again |
| I2 | `INV_I2_KEY_NAMES_PAYLOAD` | the body at `B` has the key's payload |
| I3 | `INV_I3_EDGE_BEFORE_OBSERVE` | every observing phase of a writer has its edge |
| I4 | `INV_I4_INTENT_PERMANENT` | no intent `DELETE` is sent, and none applies on an existing intent, while its token is current |
| I5, T3 (c) | `INV_I5_DELETE_EVIDENCE` | a body `DELETE` of an intent-mode round carries: confirmed intent, committed pending row, later cut at in-degree 0, `HEAD` |
| I6, T3 (b) | `INV_I6_ADOPTION` | an adoption is of a marked body, by a learned writer, after a store-answered 404 and an equal second `HEAD` |
| I7, T1 | `INV_I7_MARKED_MEANS_NEW` | no marked body, marked publication, intent or intent create on an unswitched pool |
| T3 (a) | `INV_T3A_MARKED_NEVER_MARKER_AUTHORITY` | no body `DELETE` for a marked body on a row without a confirmed intent |
| I8 | `INV_I8_COMMITTED_REF_HAS_BODY` | a committed ref has a body |
| A3 | `INV_A3_DELETE_IS_EXACT` | a `DELETE` removes only the landing its `HEAD` saw |
| §1 decision 1 | `INV_WRITERS_NEVER_MUTATE_INTENTS` | no writer sends an intent request |
| §1 budget | `INV_REQUEST_BUDGET` | reads / `PUT`s at the moment of success: first creation 1/1, clean reuse 3/0, reuse after condemnation 2/1, legacy or not learned 1/1, changed at W5 3/1, unanswered publication that landed 4/1; otherwise at most 3 reads and 1 `PUT` per loop iteration. Counters are not reset by an unanswered publication |
| §7.3, §7.4 | `FP_SWITCH_NEEDS_STAMP`, `FP_NO_GC_STATE_WITHOUT_POOL_META`, `FP_LEARNED_IS_SOUND`, `FP_SWITCHED_BEFORE_FIRST_WRITER`, `FP_NO_OLD_MEMBER_ON_SWITCHED_POOL`, `FP_DIALECT_WRITTEN_ONCE` | module `CaBlobIntentFreshPool` |

## Runs {#runs}

Base constants: 2 writers of this build, 2 GC actors of this build, pool switched and every writer learned (`Start=T3`),
seed `orphan` (a marked body with no reference), content-derived tokens, `MaxPuts=2`, `MaxVer=3`, GC faults on,
no late kind, symmetry over writers and over GC actors. `_generation` rows use tokens unique per landing.
`Late` names the request kinds that stay in flight after their sender stopped waiting: `put` body publication,
`ic` intent create, `id` intent delete, `bd` body delete. Time is TLC's own, 8 workers.

### Green, steady state {#green-steady-state}

| Configuration | Constants that differ from the base | Verdict | Distinct states | Depth | Time |
|---|---|---|---:|---:|---:|
| `safe_base` | MaxVer=4 | green | 3,639,570 | 67 | 27s |
| `safe_base_generation` | TokenKind=Generation, MaxVer=4 | green | 2,373,105 | 67 | 18s |
| `safe_late_put` | Late={put} | green | 1,875,003 | 59 | 14s |
| `safe_late_put_generation` | TokenKind=Generation, Late={put} | green | 1,390,120 | 59 | 11s |
| `safe_late_create` | Late={ic} | green | 4,130,984 | 75 | 34s |
| `safe_late_create_generation` | TokenKind=Generation, Late={ic} | green | 2,654,952 | 75 | 22s |
| `safe_late_intent_delete` | Late={id} | green | 1,360,422 | 60 | 10s |
| `safe_late_intent_delete_generation` | TokenKind=Generation, Late={id} | green | 901,450 | 60 | 7s |
| `safe_late_body_delete` | Late={bd} | green | 2,159,057 | 64 | 17s |
| `safe_late_body_delete_generation` | TokenKind=Generation, Late={bd} | green | 1,427,584 | 64 | 12s |
| `safe_refused_create` | Seed=spared, Late={ic} | green | 5,001,216 | 69 | 40s |
| `safe_refused_create_generation` | Seed=spared, TokenKind=Generation, Late={ic} | green | 3,129,378 | 69 | 26s |
| `safe_spare_recondemn` | Seed=pend, MaxVer=4 | green | 6,947,132 | 71 | 52s |
| `safe_spare_recondemn_generation` | Seed=pend, TokenKind=Generation, MaxVer=4 | green | 4,422,410 | 70 | 33s |
| `safe_lost_commit` | Seed=pend, Late={bd,id} | green | 9,636,821 | 69 | 83s |
| `safe_lost_commit_generation` | Seed=pend, TokenKind=Generation, Late={bd,id} | green | 6,557,173 | 69 | 58s |
| `safe_cleanup` | Late={id}, Fsck=True | green | 8,774,478 | 71 | 74s |
| `safe_cleanup_generation` | TokenKind=Generation, Late={id}, Fsck=True | green | 5,792,814 | 71 | 49s |
| `safe_cleanup_spared` | Seed=spared, Late={id}, Fsck=True | green | 15,544,813 | 70 | 126s |
| `safe_cleanup_spared_generation` | Seed=spared, TokenKind=Generation, Late={id}, Fsck=True | green | 9,924,261 | 70 | 81s |
| `safe_integrity` | Seed=empty, MaxPuts=3, Late={put}, StagedCopy=True, SourceChanges=True | green | 22,507,788 | 61 | 142s |
| `safe_integrity_generation` | Seed=empty, TokenKind=Generation, MaxPuts=3, Late={put}, StagedCopy=True, SourceChanges=True | green | 9,352,566 | 61 | 61s |
| `safe_budget` | Late={put}, Budget=True | green | 9,904,674 | 60 | 72s |
| `safe_budget_generation` | TokenKind=Generation, Late={put}, Budget=True | green | 7,256,041 | 60 | 53s |
| `safe_empty` | Seed=empty, MaxPuts=3, Late={put} | green | 6,703,427 | 61 | 48s |
| `safe_empty_generation` | Seed=empty, TokenKind=Generation, MaxPuts=3, Late={put} | green | 2,710,989 | 61 | 20s |
| `safe_3writers` | writers=3, gcs=1, MaxPuts=3, Late={put} | green | 2,468,337 | 66 | 22s |
| `safe_all_late` | Late={put,ic,id,bd}, Fsck=True | green | 73,509,169 | 85 | 806s |
| `safe_linker_steady` | Late={bd}, Linker=True | green | 2,542,946 | 65 | 20s |
| `safe_legacy_reupload` | Seed=legacy | green | 2,538,683 | 61 | 18s |
| `alt_adopt_changed_token` | Late={put}, Sab={alt_adopt_changed_token} | green | 1,856,661 | 59 | 14s |
| `alt_adopt_changed_token_generation` | TokenKind=Generation, Late={put}, Sab={alt_adopt_changed_token} | green | 1,380,949 | 59 | 11s |

### Green, transition {#green-transition}

`Start=T1`: floor 4, `gc/state` not stamped, pool not switched, nobody learned. Seeds: `legacy` (unmarked body with a
`Clean` marker), `legacy_cand` (candidate row, `Condemned` marker), `legacy_pend` (inherited pending row). `Builds`
names the writer builds a slot may run; `old_gcs` the GC actors of the older build.

| Configuration | Constants that differ from the base | Verdict | Distinct states | Depth | Time |
|---|---|---|---:|---:|---:|
| `safe_transition_1gc` | gcs=1, Start=T1, Seed=legacy, Builds={new,old} | green | 2,779,879 | 67 | 13s |
| `safe_transition_1gc_generation` | gcs=1, Start=T1, Seed=legacy, Builds={new,old}, TokenKind=Generation | green | 1,912,428 | 67 | 9s |
| `safe_transition_1gc_empty` | gcs=1, Start=T1, Seed=empty, Builds={new,old} | green | 816,695 | 61 | 4s |
| `safe_stale_marker_round_1gc` | gcs=1, Start=T1, Seed=legacy_cand, MaxVer=4 | green | 3,055,952 | 78 | 14s |
| `safe_inherited_demoted_1gc` | gcs=1, Start=T1, Seed=legacy_pend, MaxVer=4 | green | 2,801,026 | 79 | 13s |
| `safe_unlearned_after_switch_1gc` | gcs=1, Start=T1, Seed=legacy, MaxPuts=3 | green | 8,575,615 | 72 | 38s |
| `safe_late_old_put_1gc` | gcs=1, Start=T1, Seed=legacy, Builds={new,old}, Late={put} | green | 4,130,014 | 67 | 19s |
| `safe_late_old_delete` | writers=1, old_gcs={g2}, Start=T1, Seed=legacy_pend, Late={bd} | green | 11,426,436 | 82 | 54s |
| `safe_linker_1gc` | gcs=1, Start=T1, Seed=legacy, Linker=True | green | 1,953,304 | 70 | 9s |
| `safe_transition_cleanup_1gc` | gcs=1, Start=T1, Seed=legacy_pend, Late={id}, Fsck=True | green | 7,659,792 | 84 | 41s |
| `safe_transition_late_create_1gc` | gcs=1, Start=T1, Seed=legacy_pend, Late={ic} | green | 6,055,482 | 92 | 32s |
| `safe_transition_old_gc_1w` | writers=1, old_gcs={g2}, Start=T1, Seed=legacy | green | 2,539,582 | 65 | 10s |
| `safe_transition_old_gc_cand_1w` | writers=1, old_gcs={g2}, Start=T1, Seed=legacy_cand | green | 3,685,542 | 71 | 15s |
| `safe_transition_1gc_deep` | gcs=1, Start=T1, Seed=legacy, Builds={new,old}, MaxPuts=3, MaxVer=5, Late={put,bd} | not finished (time limit) | 213,905,982 | >=68 | 1200s |
| `safe_inherited_demoted` | Start=T1, Seed=legacy_pend | green | 117,915,257 | 80 | 915s |
| `safe_transition_old_gc` | old_gcs={g2}, Start=T1, Seed=legacy | green | 56,695,299 | 71 | 295s |
| `safe_transition` | old_gcs={g2}, Start=T1, Seed=legacy, Builds={new,old} | not finished (time limit) | 109,355,398 | >=45 | 601s |

### Fresh pool {#fresh-pool}

Module `CaBlobIntentFreshPool`: two mounts, 3 crashes (2 in the liveness row).

| Configuration | Constants that differ from the base | Verdict | Distinct states | Depth | Time |
|---|---|---|---:|---:|---:|
| `fp_safe_fresh_pool` | (base) | green | 5,761 | 22 | 0s |
| `fp_safe_two_dialects` | OtherDialect={m2} | green | 8,060 | 25 | 0s |
| `fp_safe_bad_probe` | BadProbe={m2} | green | 3,786 | 21 | 0s |
| `fp_safe_all_bad_probe` | BadProbe={m1, m2} | green | 793 | 14 | 0s |
| `fp_safe_old_creator` | OldCreator=TRUE | green | 12,689 | 23 | 0s |
| `fp_safe_old_creator_two_dialects` | OtherDialect={m2}, OldCreator=TRUE | green | 17,287 | 26 | 0s |
| `fp_live_reaches_t3` | MaxCrashes=2 | green | 2,775 | 20 | 0s |
| `fp_sab_switch_before_stamp` | Sab={switch_before_stamp} | RED: FP_SWITCH_NEEDS_STAMP | 189 | 9 | 0s |
| `fp_sab_auto_switch_upgraded` | OldCreator=TRUE, Sab={auto_switch_upgraded} | RED: FP_NO_OLD_MEMBER_ON_SWITCHED_POOL | 2,526 | 12 | 0s |
| `fp_sab_writer_before_sequence` | Sab={writer_before_sequence} | RED: FP_SWITCHED_BEFORE_FIRST_WRITER | 55 | 7 | 0s |
| `fp_sab_no_dialect_rule` | OtherDialect={m2}, Sab={no_dialect_rule} | RED: FP_LEARNED_IS_SOUND | 2,687 | 16 | 0s |
| `fp_wit_t3_after_crash_at_n2` | (base) | reached | 2,071 | 15 | 0s |
| `fp_wit_t3_after_crash_at_n3` | (base) | reached | 1,546 | 14 | 0s |
| `fp_wit_t3_after_crash_at_n0_n1` | (base) | reached | 1,735 | 14 | 0s |
| `fp_wit_late_switch_write` | (base) | reached | 696 | 12 | 0s |
| `fp_wit_foreign_dialect_refused` | OtherDialect={m2} | reached | 955 | 13 | 0s |
| `fp_wit_bad_probe_admitted_unswitched` | BadProbe={m2} | reached | 105 | 9 | 0s |
| `fp_wit_upgraded_pool` | OldCreator=TRUE | reached | 185 | 8 | 0s |
| `fp_wit_early_t2` | BadProbe={m2} | reached | 178 | 9 | 0s |

### Liveness {#liveness}

One GC actor, no GC faults, unbounded rounds, `cas-fsck` running, the code's condemn trigger (`Trigger=zero`),
no symmetry, weak fairness on every GC step, on request application, on publication landing and on `cas-fsck`.

Claimed property, `LIVE_ROW_LEAVES_AND_NO_STALE_INTENT`: once the writers are done for good, eventually no row is
left and every intent that is left names the current body. `live_no_fsck` checks the same without `cas-fsck`;
`live_orphan_body` checks `LIVE_UNREFERENCED_BODY_DELETED`; `live_intent_over_orphan` checks `LIVE_NO_INTENT_LEFT`.
The red rows are the residuals of §8.2 of the specification; the last row is green because a row-less intent
over a live body needs a GC fault or a late create, which these rows do not have.

| Configuration | Constants that differ from the base | Verdict | Distinct states | Depth | Time |
|---|---|---|---:|---:|---:|
| `live_base` | gcs=1, MaxVer=0, Faults=False, Trigger=zero, Fsck=True | green | 1,503,438 | 92 | 40s |
| `live_late` | gcs=1, MaxVer=0, Late={put,ic,id,bd}, Faults=False, Trigger=zero, Fsck=True | green | 3,434,396 | 80 | 111s |
| `live_spared` | gcs=1, Seed=spared, MaxVer=0, Late={ic}, Faults=False, Trigger=zero, Fsck=True | green | 1,497,598 | 93 | 41s |
| `live_pend` | gcs=1, Seed=pend, MaxVer=0, Late={put}, Faults=False, Trigger=zero, Fsck=True | green | 2,071,614 | 75 | 58s |
| `live_transition_small` | writers=1, gcs=1, Start=T1, Seed=legacy_pend, MaxPuts=1, MaxVer=0, Faults=False, Trigger=zero, Fsck=True | green | 117,012 | 81 | 2s |
| `live_transition_legacy_small` | writers=1, gcs=1, Start=T1, Seed=legacy, MaxPuts=1, MaxVer=0, Faults=False, Trigger=zero, Fsck=True | green | 165,699 | 91 | 4s |
| `live_transition` | gcs=1, Start=T1, Seed=legacy_pend, MaxVer=0, Faults=False, Trigger=zero, Fsck=True | not finished (time limit) | 18,096,580 | >=75 | 601s |
| `live_no_fsck` | gcs=1, MaxVer=0, Late={put}, Faults=False, Trigger=zero | RED: LIVE_ROW_LEAVES_AND_NO_STALE_INTENT | 524,395 | 80 | 10s |
| `live_orphan_body` | gcs=1, MaxVer=0, Late={put}, Faults=False, Trigger=zero, Fsck=True | RED: LIVE_UNREFERENCED_BODY_DELETED | 116,101 | 30 | 3s |
| `live_intent_over_orphan` | gcs=1, MaxVer=0, Late={put,ic}, Faults=False, Trigger=zero, Fsck=True | green | 2,078,118 | 85 | 58s |

### Controls and witnesses {#controls-and-witnesses}

A control is a sabotage of A1 on a store whose tokens are unique per landing: it must stay green. A witness is an
invariant that must fail: the scenario is reachable in the model without sabotage. Three run under a
named sabotage, to show that a sabotage which stays green does fire: `wit_derived_row_deletes`,
`wit_alt_adopt_changed`, `wit_inherited_executed`.

| Configuration | Constants that differ from the base | Verdict | Distinct states | Depth | Time |
|---|---|---|---:|---:|---:|
| `ctl_resend_generation` | TokenKind=Generation, MaxResends=1, Sab={resend_envelope} | green | 6,917,443 | 61 | 51s |
| `ctl_alias_two_copies_generation` | Seed=empty, TokenKind=Generation, StagedCopy=True, Sab={staging_alias_two_copies} | green | 1,140,746 | 56 | 8s |
| `wit_adoption` | Budget=True | reached | 112 | 9 | 0s |
| `wit_publish_over_intent` | Seed=spared | reached | 176 | 9 | 0s |
| `wit_spare_standing_intent` | (base) | reached | 2,201 | 17 | 0s |
| `wit_recondemn_on_existing_intent` | Seed=spared | reached | 734 | 13 | 0s |
| `wit_late_create_after_row_left` | Late={ic} | reached | 1,977 | 16 | 0s |
| `wit_stale_intent_cleaned` | Seed=spared, Fsck=True | reached | 213 | 9 | 0s |
| `wit_legacy_reupload` | Start=T1, Seed=legacy | reached | 2,690 | 12 | 0s |
| `wit_changed_at_w5` | Budget=True | reached | 4,229 | 18 | 0s |
| `wit_deposed_delete_lands` | (base) | reached | 9,269 | 21 | 0s |
| `wit_body_deleted` | (base) | reached | 4,214 | 18 | 0s |
| `wit_intent_delete_after_body` | (base) | reached | 4,140 | 18 | 0s |
| `wit_two_absent_publishers` | Seed=empty | reached | 192 | 9 | 0s |
| `wit_staged_copy` | Seed=empty, StagedCopy=True | reached | 73 | 7 | 0s |
| `wit_supersede` | (base) | reached | 3,512 | 18 | 0s |
| `wit_inherited_demoted` | Start=T1, Seed=legacy_pend | reached | 4,158 | 12 | 0s |
| `wit_marker_round_after_switch` | Start=T1, Seed=legacy | reached | 543 | 9 | 0s |
| `wit_marked_condemned_by_marker_round` | Start=T1, Seed=legacy | reached | 16,075 | 15 | 0s |
| `wit_late_old_put` | old_gcs={g2}, Start=T1, Seed=legacy, Builds={new,old}, Late={put} | reached | 41,292 | 13 | 0s |
| `wit_late_old_delete` | old_gcs={g2}, Start=T1, Seed=legacy_pend, Builds={new,old}, Late={bd} | reached | 4,110 | 9 | 0s |
| `wit_link` | Linker=True | reached | 107 | 9 | 0s |
| `wit_unlearned_after_switch` | Start=T1, Seed=legacy | reached | 319 | 8 | 0s |
| `wit_t3_adoption` | Start=T1, Seed=legacy, Budget=True | reached | 53,149 | 16 | 0s |
| `wit_derived_row_deletes` | Seed=spared, Sab={candidate_from_intent} | reached | 699 | 13 | 0s |
| `wit_alt_adopt_changed` | Late={put}, Budget=True, Sab={alt_adopt_changed_token} | reached | 8,658 | 20 | 0s |
| `wit_inherited_executed` | Start=T1, Seed=legacy_pend, Sab={inherited_executed} | reached | 3,616 | 12 | 0s |
| `wit_old_writer_adopts_after_switch` | gcs=1, Start=T1, Seed=legacy, Builds={new,old} | reached | 4,997 | 12 | 0s |
| `wit_unanswered_put_adopted` | Seed=empty, Budget=True | reached | 431 | 10 | 0s |
| `wit_delete_misses` | (base) | reached | 7,187 | 20 | 0s |


## Sabotages {#sabotages}

One switch each. Plain name: every invariant on, the first that falls. `_L`: only I8, the data-loss schedule.

| Sabotage | Rule removed | §4.3 | First invariant to fall (distinct; depth) | I8 alone |
|---|---|---|---|---|
| `sab_no_second_head` | W5 | S1 | `INV_I6_ADOPTION` (55; depth 7) | RED (16,792; depth 25) |
| `sab_attempted_create_confirms` | G3, table "what confirms" | S4 | `INV_I5_DELETE_EVIDENCE` (892; depth 15) | RED (7,552; depth 19) |
| `sab_absent_conflict_confirms` | G3, table "what confirms" | — | `INV_I5_DELETE_EVIDENCE` (1,497; depth 15) | RED (12,859; depth 20) |
| `sab_delete_on_graduation_cut` | G3, G4: delete a round after the commit | — | `INV_I5_DELETE_EVIDENCE` (668; depth 15) | RED (3,067; depth 18) |
| `sab_spare_deletes_intent` | G6, G9 | S3 | `INV_I4_INTENT_PERMANENT` (105; depth 9) | RED (10,955; depth 17) |
| `sab_ambiguous_delete_cleans_intent` | G5 | S9 | `INV_I4_INTENT_PERMANENT` (2,863; depth 18) | RED (21,218; depth 22) |
| `sab_delete_despite_indegree` | G4, G6 | — | `INV_I5_DELETE_EVIDENCE` (119; depth 9) | RED (185,757; depth 27) |
| `sab_writer_deletes_intent` | §1 decision 1 | — | `INV_I4_INTENT_PERMANENT` (60; depth 7) | RED (8,100; depth 15) |
| `sab_cleanup_head_first` | F2 order | S2 | `INV_I4_INTENT_PERMANENT` (109,058; depth 25) | RED (2,497,378; depth 36) |
| `sab_cleanup_raw_spelling` | §6.1 canonical identity | S8 | `INV_I4_INTENT_PERMANENT` (712; depth 15) | RED (38,879; depth 22) |
| `sab_cleanup_foreign_dialect` | F0 | S10 | `INV_I4_INTENT_PERMANENT` (723; depth 15) | RED (40,652; depth 23) |
| `sab_gc_raw_spelling` | §6.1 canonical identity, in G4 and G7 | — | `INV_I4_INTENT_PERMANENT` (2,421; depth 15) | RED (77,110; depth 24) |
| `sab_stale_read` | W8 | S12 | `INV_I6_ADOPTION` (119; depth 8) | RED (34,629; depth 23) |
| `sab_resend_envelope` | A1: one send | — | `INV_I1_NO_RECURRENCE` (9,279; depth 21) | RED (2,098,462; depth 37) |
| `sab_staging_alias_two_copies` | A1: shared once-only flag | — | `INV_I1_NO_RECURRENCE` (88,508; depth 25) | RED (300,674; depth 30) |
| `sab_unchecked_stream` | §6.3 | S7 | `INV_I2_KEY_NAMES_PAYLOAD` (1,377; depth 15) | **green** (2,569,296; depth 60; 18s) |
| `sab_adopt_unmarked` | W3 | — | `INV_I6_ADOPTION` (1,430; depth 11) | RED (51,707; depth 16) |
| `sab_mark_before_learned` | §3.1 mark bit | — | `INV_I7_MARKED_MEANS_NEW` (95; depth 6) | RED (2,216,181; depth 29) |
| `sab_no_stamp` | §7.3: stamp before the switch | — | `INV_T3A_MARKED_NEVER_MARKER_AUTHORITY` (4,644,647; depth 24) | RED (23,475,724; depth 30) |
| `sab_old_gc_after_stamp` | §2.4: header 4 stops the older GC | — | `INV_T3A_MARKED_NEVER_MARKER_AUTHORITY` (8,252,663; depth 26) | RED (38,791,894; depth 31) |
| `sab_lost_stamp_old_bootstrap` | A9 (X2) | — | `INV_T3A_MARKED_NEVER_MARKER_AUTHORITY` (13,519,459; depth 27) | RED (72,201,755; depth 32) |
| `sab_mode_cached` | G1: every round reads again | — | `INV_T3A_MARKED_NEVER_MARKER_AUTHORITY` (1,636,479; depth 26) | RED (7,081,412; depth 31) |
| `sab_mode_on_failed_read` | G1: no fallback to marker mode | S11 | `INV_T3A_MARKED_NEVER_MARKER_AUTHORITY` (1,298,216; depth 26) | RED (7,673,222; depth 32) |
| `sab_switch_with_old_writer` | A8 (X1) | — | `INV_I8_COMMITTED_REF_HAS_BODY` (4,551,391; depth 27) | RED (4,807,813; depth 27) |
| `sab_inherited_executed` | G8 | — | `INV_I5_DELETE_EVIDENCE` (1,164; depth 10) | **green** (63,900,752; depth 74; 439s) |
| `sab_candidate_from_intent` | G9: no row from an intent | — | **green** (2,066,296; depth 59) | **green** (2,066,296; depth 59; 14s) |


`sab_unchecked_stream_L` is green because I8 says "has a body", not "has the right payload"; the payload is I2.

## Reduced traces {#reduced-traces}

Actors: `W`, `W1`, `W2` writers; `G1`, `G2` GC actors; `F` `cas-fsck`; `S` the store. `t`, `t1`, `t2` are body
incarnations. Every trace is the loss run (`_L`, only I8 checked) unless said otherwise; "pipeline to pending" means
condemn `t` and commit, then create `I(B,t)`, get the answer, commit the row with `pending` and `intent`.

**`sab_no_second_head`** (S1, W5 removed)

1. `G1`: pipeline to pending for `t`; next round, cut with no edge.
2. `W`: edge; `HEAD` sees `t`.
3. `G1`: `HEAD` sees `t`; `DELETE If-Match t` applies; intent `DELETE` applies.
4. `W`: `GET I(B,t)` answers 404; adopts with no second `HEAD`; commits. The ref names a key with no body.

**`sab_attempted_create_confirms`** (S4)

1. `G1`: condemn `t`, commit. Next round: sends the create and counts the send as a confirmation; commits pending.
   The create never applies.
2. `G1`: next round, cut with no edge.
3. `W`: edge; `HEAD` `t`; `GET` 404; `HEAD` `t`; adopts; commits.
4. `G1`: `HEAD` `t`; `DELETE` applies.

**`sab_absent_conflict_confirms`**

1. `G1`: condemn `t`, commit. Next round: create sent, unanswered, never applied; the resolve read finds no object;
   that is counted as a confirmation; commits pending.
2. Steps 2 to 4 of the trace above.

**`sab_delete_on_graduation_cut`**

1. `G1`: condemn `t`, commit. Next round: cut with no edge.
2. `W`: edge; `HEAD` `t`; `GET` 404; `HEAD` `t`; adopts; commits.
3. `G1`, same round: create applies and is answered; goes on to `HEAD` `t` and `DELETE`, which applies. The cut is
   older than the edge.

**`sab_spare_deletes_intent`** (S3; seed: a committed pending row with its intent)

1. `G1`: round, cut with no edge.
2. `W`: edge; `HEAD` `t`.
3. `G1`: `HEAD` `t`; `DELETE` sent, in flight.
4. `G2`: round, its cut sees the edge of `W`; spares the row and deletes the intent; the delete applies.
5. `W`: `GET` 404; `HEAD` `t`; adopts; commits.
6. `S`: the `DELETE` of `G1` applies.

**`sab_ambiguous_delete_cleans_intent`** (S9; body `DELETE` may land late)

1. `G1`: pipeline to pending for `t`; next round, cut with no edge.
2. `W`: edge; `HEAD` `t`.
3. `G1`: `HEAD` `t`; `DELETE` sent, unanswered; sends the intent `DELETE` all the same; it applies.
4. `W`: `GET` 404; `HEAD` `t`; adopts; commits.
5. `S`: the `DELETE` applies.

**`sab_delete_despite_indegree`** (seed: pending row with intent for `t1`)

1. `W`: edge; `HEAD` `t1`; meets the intent; publishes `t2`; abandons.
2. `G1`: supersede, candidate `t2`, commit. Next round: cut with no edge; create `I(B,t2)` sent.
3. `W`: edge; `HEAD` `t2`; `GET` 404; `HEAD` `t2`; adopts; commits.
4. `G1`: the create applies and is answered; commits pending. Next round: the cut SEES the edge of `W`; the row is
   executed anyway: `HEAD` `t2`, `DELETE` applies.

**`sab_writer_deletes_intent`** (seed: pending row with intent for `t`)

1. `G1`: lease read (pending `t`).
2. `W`: edge; `HEAD` `t`; meets the intent; sends its own intent `DELETE`; abandons before it publishes.
3. `G1`: cut with no edge.
4. `W`: new edge; `HEAD` `t`; the intent `DELETE` applies; `GET` 404; `HEAD` `t`; adopts; commits.
5. `G1`: `HEAD` `t`; `DELETE` applies.

**`sab_cleanup_head_first`** (S2)

1. `G1`: candidate `t1`; create `I(B,t1)` applies. `W`: edge; `HEAD` `t1`; meets the intent; publication of `t2` sent.
2. `F`: `HEAD B` first: sees `t1`.
3. `S`: the publication lands: the body is `t2`. `W` abandons.
4. `G2`: supersede, candidate `t2`, commit; create `I(B,t2)`, confirmed, commit pending; next round, cut with no edge.
5. `W`: edge; `HEAD` `t2`.
6. `F`: `GET` finds `I(B,t2)`; `t2` is not what its old `HEAD` showed; calls it stale; deletes it.
7. `W`: `GET` 404; `HEAD` `t2`; adopts; commits.
8. `G2`: `HEAD` `t2`; `DELETE` applies.

**`sab_cleanup_raw_spelling`** (S8) and **`sab_cleanup_foreign_dialect`** (S10): the same trace

1. `G1`: pipeline to pending for `t`; next round, cut with no edge. `W`: edge; `HEAD` `t`. `G1`: `HEAD` `t`; `DELETE`
   sent, in flight.
2. `F`: `GET I(B,t)` present; `HEAD B`: the same incarnation reads as another identity; deletes `I(B,t)`.
3. `W`: `GET` 404; `HEAD` `t`; adopts; commits.
4. `S`: the `DELETE` applies.

**`sab_gc_raw_spelling`** (the same defect inside GC, at G7)

1. Pending row with intent for `t` is committed. `G1` and `G2` start rounds.
2. `G1`: cut; its `HEAD` renders `t` differently; G7 reads "replaced": candidate again, delete of `I(B,t)` sent.
3. `G2`: cut with no edge. `W`: edge; `HEAD` `t`. The intent delete applies. `W`: `GET` 404; `HEAD` `t`; adopts; commits.
4. `G2`: `HEAD` `t`; `DELETE` applies.

**`sab_stale_read`** (S12)

1. `W`, before any edge: observes "`I(B,t)` absent" and keeps the answer.
2. `G1`: pipeline to pending for `t`; next round, cut with no edge.
3. `W`: edge; `HEAD` `t`; W4 is answered from the kept observation; `HEAD` `t`; adopts; commits.
4. `G1`: `HEAD` `t`; `DELETE` applies.

**`sab_resend_envelope`** (A1; content-derived tokens; first fall: I1)

1. `G1`: candidate `t1`; create `I(B,t1)` applies. `W`: edge; `HEAD` `t1`; meets the intent; publishes `t2`; abandons.
2. `G2`: supersede, candidate `t2`; create `I(B,t2)`, confirmed, commit pending; next round, cut with no edge.
3. `W`: edge; `HEAD` `t2`; meets `I(B,t2)`; publishes `t3`, acknowledged; commits.
4. `S`: the transport re-applies the publication of step 1: the body is `t2` again (I1 falls).
5. `G2`: `HEAD` shows `t2`, the row's identity; `DELETE If-Match t2` applies. The committed ref has no body.

**`sab_staging_alias_two_copies`** (A1; content-derived tokens; first fall: I1)

1. `W1`: edge; `HEAD` absent; verbatim copy of the staged envelope `E` lands; abandons.
2. `G1`: pipeline to pending for `E`; next round: `HEAD` `E`, `DELETE` sent. `G2`: round on the same committed row:
   `HEAD` `E`, a second `DELETE If-Match E` sent.
3. `S`: the first `DELETE` applies.
4. `W2`: edge; `HEAD` absent; its alias copies the same staged envelope: `E` is the body again (I1 falls);
   acknowledged; commits.
5. `S`: the second `DELETE` applies and removes the new body.

**`sab_unchecked_stream`** (S7; falls on I2, not on I8)

1. `G1`: candidate `t1`; create `I(B,t1)` applies.
2. `W`: edge; `HEAD` `t1`; meets the intent; streams a source that no longer hashes to the key; it lands. The body at
   `B` has another payload.

**`sab_adopt_unmarked`** (seed: legacy body `t` under an inherited pending row)

1. `G1`: lease; mode read before the switch: marker mode; cut with no edge; `HEAD` `t`; `DELETE` sent.
2. `W`: edge. Operator: stamp, switch. `W` learns.
3. `W`: `HEAD` `t` (unmarked, taken as marked); `GET` 404; `HEAD` `t`; adopts; commits.
4. `S`: the `DELETE` applies.

**`sab_mark_before_learned`** (seed: legacy body with `Clean`; first fall: I7, a marked body on an unswitched pool)

1. `W`: edge; `HEAD` legacy; publishes `t2` MARKED before the switch; abandons.
2. `G1`, marker mode: condemn `t2` (writes `Condemned`), commit. Stamp. Next round, marker mode: graduates on the
   marker, commit. Next round, mode read still before the switch: marker mode; cut with no edge; `HEAD` `t2`;
   `DELETE` sent.
3. `W`: edge. Switch. `W` learns; `HEAD` `t2`; `GET` 404; `HEAD` `t2`; adopts; commits.
4. `S`: the `DELETE` applies.

**`sab_no_stamp`** (GC `g1` of this build, `g2` of the older build; first fall: T3 (a))

1. `G1`: lease; mode read before the switch: marker mode; cut.
2. Switch, with no stamp. `W` learns; edge; `HEAD` legacy; publishes `t2` marked; abandons.
3. `G1`: condemn `t2` (writes `Condemned`); its commit wins, because `gc/state` was not rewritten.
4. `G2` (older build): graduates on the marker, commit. Next round: cut with no edge.
5. `W`: edge; `HEAD` `t2`; `GET` 404; `HEAD` `t2`; adopts; commits.
6. `G2`: `HEAD` `t2`; `DELETE` applies.

**`sab_old_gc_after_stamp`**: as `sab_no_stamp`, with the stamp present and the older GC starting rounds on it.

**`sab_lost_stamp_old_bootstrap`** (X2): stamp, switch, a marked `t2` is published and condemned by a marker-mode
round of `G1`; `gc/state` loses its generation; the commit of `G1` wins; `G2` (older build) graduates on the marker
and deletes while `W` adopts.

**`sab_mode_cached`** and **`sab_mode_on_failed_read`** (S11; one GC actor of this build is enough)

1. Stamp. `G1`: lease; mode read before the switch: marker mode (legitimate); cut.
2. Switch. `W` learns; edge; `HEAD` legacy; publishes `t2` marked; abandons.
3. `G1`: condemn `t2` (writes `Condemned`), commit.
4. `G1`, next round: takes marker mode from its cache (or from a failed read); graduates on the marker, commit.
5. `G1`, next round: marker mode again; cut with no edge.
6. `W`: edge; `HEAD` `t2`; `GET` 404; `HEAD` `t2`; adopts; commits.
7. `G1`: `HEAD` `t2`; `DELETE` applies.

**`sab_switch_with_old_writer`** (X1, A8 broken: a writer of the older build STARTS after the switch)

1. Stamp, switch. Seed: legacy body `t` with its own `Clean` marker.
2. `G1`, intent mode: condemn `t` (no marker write), commit; create `I(B,t)`, confirmed, commit pending; next
   round, cut with no edge.
3. Older `W`: edge; `HEAD` `t`; `GET .meta` answers `Clean`; adopts; commits. It never reads the intent.
4. `G1`: `HEAD` `t`; `DELETE` applies.

The body in this trace is a LEGACY body with its own, never stale, `Clean` marker.

**`sab_inherited_executed`** (G8 removed; falls on I5 and T3 (c) only)

1. Seed: legacy body `t`, inherited pending row without `intent`. Stamp. `G1`: lease. Switch. `G1`: intent mode; cut
   with no edge; `HEAD` `t`; `DELETE` sent on marker authority by an intent-mode round.

No committed ref loses its body in any run of this sabotage (63,900,752 distinct states, complete).


## Cut or not finished {#cut}

| Configuration | What happened | What stands in for it |
|---|---|---|
| `safe_transition` (both writer builds, GC of both builds, two writers) | not finished (time limit), 109,355,398 distinct states, depth >=45, 601s, no violation | `safe_transition_1gc` (both writer builds, one GC of the new build), `safe_transition_old_gc` and `safe_transition_old_gc_1w` (GC of both builds, writers of the new build) |
| `safe_late_old_requests` (the same with late `put` and `bd`) | stopped at 220,021,663 distinct states, depth 41, 17 minutes, no violation | `safe_late_old_put_1gc` (late publication of an older writer), `safe_late_old_delete` (late delete of the older GC) |
| Transition rows with two GC actors of the new build | 57 to 118 million states each; two were run (`safe_inherited_demoted`, `safe_transition_old_gc`) | the `_1gc` rows; the deposed-leader dimension is covered by the steady-state rows |
| `safe_transition_1gc_deep` (both writer builds, one GC, 3 publications, 5 commits, late `put` and `bd`) | not finished (time limit), 213,905,982 distinct states, depth >=68, 1200s, no violation | `safe_transition_1gc`, `safe_late_old_put_1gc` |
| Crash of a writer as its own action | not a separate action | `WAbandon` at every phase; the stalled phase stands for the interval in which the dead process still owns its edge; its requests stay in flight with `put` in `Late` |
| Three writers | one row, one GC actor (`safe_3writers`) | — |
| `live_transition` (two writers, two publications, from an inherited row) | not finished (time limit), 18,096,580 distinct states, depth >=75, 601s, no violation | `live_transition_small`, `live_transition_legacy_small` (one writer, one publication) |
| Liveness with GC faults | not run: with faults always possible no progress can be claimed without strong fairness | the no-fault rows |

## Bounds, and what the model does not prove {#bounds}

- **One blob key.** No batch of intent deletes across rows, no budget, no read-ahead, no shards, no namespaces.
- **Small numbers.** 2 writers (3 in one row), 1 or 2 GC actors, 2 or 3 publications, 3 to 6 round commits. Builds per
  writer slot are unbounded.
- **A read is one atomic observation; a cut is complete and atomic.** A7 is assumed. A cut that misses an edge is
  outside the model.
- **Safety runs over-approximate the condemn trigger**: GC may condemn whenever a cut shows in-degree 0. Liveness runs
  use the code's trigger, a fresh transition to zero.
- **The older build's marker protocol is reduced** to three marker states with no round stamp, and its marker writes
  are one step with the read that precedes them. It is the environment of the transition, not its subject. A late
  `Clean` write of an older writer is therefore not in this model. After the switch nobody reads `.meta` except G4 (4).
- **Token spelling** is one switch per comparison site, not a model of renderings.
- **Not modelled:** sizes and the envelope-length check; malformed intent bytes; a token that does not canonicalise;
  multipart transport; the mark probe (a mount either learns or does not); mount slots and the checks of the switch
  command; two commands at once (two creators are in the fresh-pool module); `SYSTEM CAS GC REBUILD`; a row derived
  from a `LIST`; a store that loses or invents the mark, beyond the sabotage `mark_before_learned`.
- **Liveness not claimed:** an unreferenced body is deleted (`live_orphan_body` is red: a publication that lands
  after the round that folded its writer's withdrawal is never condemned); stale intents disappear without `cas-fsck`
  (`live_no_fsck` is red). Nothing is claimed under GC faults.
- **A green run is a statement about these bounds.** It is evidence for the rules, not a proof of the implementation.
  The table [rule to action](#mapping) is what a code review can check against.
