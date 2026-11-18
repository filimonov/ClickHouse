---
description: 'TLC evidence for CAS blob publication without a birth marker: the two-read writer and GC-owned deletion intents (rules G2 to G12). Green configurations, sabotages, witnesses, liveness, bounds, runs not made.'
sidebar_label: 'CAS blob intents TLC results'
sidebar_position: 5
slug: /superpowers/models/CaBlobIntentProtocol-results
title: 'CAS blob deletion intents TLC results'
doc_type: 'reference'
---

# `CaBlobIntentProtocol` results {#cablobintentprotocol-results}

`CaBlobIntentProtocol.tla` models §3.1 (writer W1 to W7) and §3.2 (GC G2 to G12) of
`docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md` at commit `9296f2e2f0d`. With
`Sab = {}` the module is the protocol as specified. The migration of §7 is `CaBlobIntentMigration.tla`.

Run one configuration from this directory:

```
java -Xmx16g -XX:+UseParallelGC -cp ../../../tmp/tla2tools.jar tlc2.TLC -noGenerateSpecTE -workers 8 \
    -config CaBlobIntentProtocol_safe_base.cfg CaBlobIntentProtocol.tla
```

## Verdict {#verdict}

- **Safety, complete, content dialect:** green with 2 writers, 2 GC actors, one publication in total
  (`MaxPuts = 1`), 3 round commits: base, pending seed, late intent delete, late body delete, rebuild. Also
  green: pending seed with `MaxPuts = 2`, 2 round commits; 3 writers, 1 GC actor, `MaxPuts = 3`, 3 commits;
  3 writers, 2 GC actors, `MaxPuts = 1`, 3 commits.
- **Safety, complete, generation dialect:** green with 2 writers, 2 GC actors, `MaxPuts = 2`, 3 commits: base
  and pending seed. Green with `MaxPuts = 1`: the same five runs as the content dialect. With one publication
  the two dialects give the same state graph (one landing, one token).
- **Cut by the 900 s timeout, no violation:** content dialect with `MaxPuts = 2`, 3 commits (base, pending,
  late intent delete); late intent delete and rebuild on the generation dialect.
- **Publication integrity (§6.2, A1):** green on both dialects with the verbatim staged copy and a source that
  changes after staging, 2 writers, 1 GC actor, `MaxPuts = 2`, 2 commits. With 2 GC actors both dialects were cut
  at 600 s, about 82 M states, no violation. A streamed publication left unhashed is red on
  `INV_I2_KEY_NAMES_PAYLOAD`, both dialects. A transport resend of a landed publication is red on
  `INV_I1_NO_RECURRENCE` with content tokens; with generation tokens a resend is a new landing and a new token, so
  no recurrence (cut at 300 s, no violation).
- **Also green, complete, `MaxPuts = 1`:** a lost intent adopted with a late intent delete, both dialects; a stale
  G9 listing with a late intent delete.
- **Sabotages:** every schedule S1 to S7 and S9 is red on `INV_I8_COMMITTED_REF_HAS_BODY`, both dialects.
- **Liveness:** green, also with one fault and late creates, intent deletes and body deletes. Red when G9 may win
  over G2, when a decision `HEAD` is read before the cut, without the supersede `HEAD` on a touched key, and
  with G8 before G7. The late `PUT` of §8.2 is red, as the spec accepts.
- **Witnesses:** all reached.
- **Spec and model:** no difference found. Every run used the final G6: every row kind is dropped at in-degree
  above 0. The old shape exists only as the contrast `keep_standing_on_spare`, which did not run.

## What the model checks {#model}

| Rule | Model |
|---|---|
| W2, W4 | `HEAD B`, then `GET I(B,t)` after its answer. Absent body → publish. Intent present → publish. Answered 404 → adopt |
| W5 | one unconditional publication per attempt, fresh envelope; unanswered → loop from W2; may land late (`Late` has `put`) |
| G2 | no row, zero, a fresh transition: `HEAD B`, create `I(B,t)` in the fold; confirmed → `pending(t)`, else `candidate(t)` |
| G3 | `candidate(t)` at zero, no G7: the create again |
| G4 | `pending(t)` at zero, no G7: `HEAD B`; same → `DELETE B If-Match`; `Removed`, `Gone`, `Mismatch` → `standing(t)`; absent → `standing(t)`; another token → G7 |
| G5 | an unanswered or refused `HEAD`, body `DELETE` or intent `DELETE` fails the round |
| G6 | in-degree above 0: every row is dropped, no request |
| G7 | a row of `t` at zero whose `HEAD` (pending, or touched key) shows `u`: `PUT I(B,u)` in the fold, `DELETE I(B,t)` in the batch, one commit of `pending(u)` or `candidate(u)` |
| G8 | `standing(t)` at zero, no G7: `DELETE I(B,t)`, no `HEAD` of its own |
| G9 | a row-less key at zero that G2 does not condemn in this round: one listed intent, `HEAD B`; same → `candidate(t)`, else `standing(t)`; nothing deleted |
| G12 | every decision `HEAD` is read after the cut |
| Budget | `OverBudget`: a create of G2, G3 or G7 may be skipped, giving `candidate` |
| Rebuild | `gc/state` loses its row once; intents stay |
| Faults | unbounded in safety runs; `FaultBudget` in liveness runs |

Invariants: I1, I2, I3, I4, I5, A3, I6, I8 (`INV_I8_COMMITTED_REF_HAS_BODY`), the writer and GC request
budgets. Liveness, once writers stop: `LIVE_UNREFERENCED_BODY_DELETED`, `LIVE_NO_INTENT_LEFT`.

Bounds: one key. Safety runs use `Trigger = "any"`: each round may see a fresh transition and a touched key, a
superset of the code. Liveness runs use one GC actor and the exact trigger.

## Runs {#runs}

Expected red: `sab_*` (one rule weakened), `wit_*` (a reachable state), `live_sab_*`, `live_late_put`. Every run
uses the final G6. Time is TLC's run time.

| Configuration | Bounds | Expected | Verdict | Distinct states | Time |
|---|---|---|---|---|---|
| `safe_base` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 3, faults unbounded | green | cut at 125,471,854 states, no violation | 125,471,854 | 903s |
| `safe_base_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 3, faults unbounded | green | green | 107536428 | 767s |
| `safe_pend` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 3, faults unbounded | green | cut at 123,708,382 states, no violation | 123,708,382 | 902s |
| `safe_pend_generation` | 2W 2G, generation, seed pend, MaxPuts 2, MaxVer 3, faults unbounded | green | green | 93970217 | 663s |
| `safe_late_intent_delete` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 3, late id, faults unbounded | green | cut at 122,189,711 states, no violation | 122,189,711 | 906s |
| `safe_late_intent_delete_generation` | 2W 2G, generation, seed pend, MaxPuts 2, MaxVer 3, late id, faults unbounded | green | cut at 118,056,575 states, no violation | 118,056,575 | 906s |
| `safe_rebuild_generation` | 2W 2G, generation, seed pend, MaxPuts 2, MaxVer 3, faults unbounded, Rebuild | green | cut at 111,950,009 states, no violation | 111,950,009 | 900s |
| `safe_p1_base` | 2W 2G, content, seed orphan, MaxPuts 1, MaxVer 3, faults unbounded | green | green | 11528263 | 82s |
| `safe_p1_base_generation` | 2W 2G, generation, seed orphan, MaxPuts 1, MaxVer 3, faults unbounded | green | green | 11528263 | 138s |
| `safe_p1_pend` | 2W 2G, content, seed pend, MaxPuts 1, MaxVer 3, faults unbounded | green | green | 9383859 | 66s |
| `safe_p1_pend_generation` | 2W 2G, generation, seed pend, MaxPuts 1, MaxVer 3, faults unbounded | green | green | 9383859 | 71s |
| `safe_p1_late_intent_delete` | 2W 2G, content, seed pend, MaxPuts 1, MaxVer 3, late id, faults unbounded | green | green | 17199980 | 131s |
| `safe_p1_late_intent_delete_generation` | 2W 2G, generation, seed pend, MaxPuts 1, MaxVer 3, late id, faults unbounded | green | green | 17199980 | 134s |
| `safe_p1_late_body_delete` | 2W 2G, content, seed orphan, MaxPuts 1, MaxVer 3, late bd, faults unbounded | green | green | 22750900 | 175s |
| `safe_p1_late_body_delete_generation` | 2W 2G, generation, seed orphan, MaxPuts 1, MaxVer 3, late bd, faults unbounded | green | green | 22750900 | 179s |
| `safe_p1_rebuild` | 2W 2G, content, seed pend, MaxPuts 1, MaxVer 3, faults unbounded, Rebuild | green | green | 21310596 | 153s |
| `safe_p1_rebuild_generation` | 2W 2G, generation, seed pend, MaxPuts 1, MaxVer 3, faults unbounded, Rebuild | green | green | 21310596 | 228s |
| `safe_v2_pend` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 2, faults unbounded | green | green | 63130660 | 464s |
| `safe_3writers_1gc` | 3W 1G, content, seed pend, MaxPuts 3, MaxVer 3, faults unbounded | green | green | 13536066 | 153s |
| `sab_delete_on_graduation_cut` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab delete_on_graduation_cut | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 3739 | 0s |
| `sab_delete_on_graduation_cut_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab delete_on_graduation_cut | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 3711 | 0s |
| `sab_attempted_create_confirms` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab attempted_create_confirms | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 8504 | 0s |
| `sab_attempted_create_confirms_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab attempted_create_confirms | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 8359 | 0s |
| `sab_spare_deletes_intent` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab spare_deletes_intent | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 131002 | 1s |
| `sab_spare_deletes_intent_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab spare_deletes_intent | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 130553 | 1s |
| `sab_id_in_body_job` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab id_in_body_job | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 50923 | 1s |
| `sab_id_in_body_job_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab id_in_body_job | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 51497 | 0s |
| `sab_dfr_despite_indegree` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab dfr_despite_indegree | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 230978 | 2s |
| `sab_dfr_despite_indegree_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab dfr_despite_indegree | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 243534 | 2s |
| `sab_cut_before_commit` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab cut_before_commit | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 738787 | 4s |
| `sab_cut_before_commit_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab cut_before_commit | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 739556 | 4s |
| `sab_adopt_delete_now` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded, Rebuild, Sab adopt_delete_now | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 88998 | 1s |
| `sab_adopt_delete_now_generation` | 2W 2G, generation, seed pend, MaxPuts 2, MaxVer 4, faults unbounded, Rebuild, Sab adopt_delete_now | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 83677 | 1s |
| `sab_ambiguous_delete_standing` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab ambiguous_delete_standing | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 854093 | 5s |
| `sab_ambiguous_delete_standing_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab ambiguous_delete_standing | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 776232 | 4s |
| `sab_delete_despite_indegree` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab delete_despite_indegree | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 15129 | 0s |
| `sab_delete_despite_indegree_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab delete_despite_indegree | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 15007 | 0s |
| `sab_adopt_delete_now_lost_commit` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, late bd, faults unbounded, Sab adopt_delete_now | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 118220 | 1s |
| `sab_adopt_delete_now_lost_commit_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 4, late bd, faults unbounded, Sab adopt_delete_now | red | red: `INV_I8_COMMITTED_REF_HAS_BODY` | 108091 | 1s |
| `sab_absent_conflict_confirms` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab absent_conflict_confirms | red | red: `INV_I5_DELETE_EVIDENCE` | 2034 | 0s |
| `sab_stale_read` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Sab stale_read | red | red: `INV_I6_ADOPTION` | 189 | 0s |
| `sab_staging_alias_two_copies` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 2, faults unbounded, StagedCopy, Sab staging_alias_two_copies | red | red: `INV_I1_NO_RECURRENCE` | 23068032 | 130s |
| `safe_pub_integrity_1gc` | 2W 1G, content, seed orphan, MaxPuts 2, MaxVer 2, faults unbounded, StagedCopy, SourceChanges | green | green | 821698 | 4s |
| `safe_pub_integrity_1gc_generation` | 2W 1G, generation, seed orphan, MaxPuts 2, MaxVer 2, faults unbounded, StagedCopy, SourceChanges | green | green | 556370 | 3s |
| `safe_pub_integrity` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 2, faults unbounded, StagedCopy, SourceChanges | green | cut at 84,219,868 states, no violation | 84,219,868 | 600s |
| `safe_pub_integrity_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 2, faults unbounded, StagedCopy, SourceChanges | green | cut at 81,987,738 states, no violation | 81,987,738 | 600s |
| `sab_unchecked_stream` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 2, faults unbounded, SourceChanges, Sab unchecked_stream | red | red: `INV_I2_KEY_NAMES_PAYLOAD` | 2120 | 0s |
| `sab_unchecked_stream_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxVer 2, faults unbounded, SourceChanges, Sab unchecked_stream | red | red: `INV_I2_KEY_NAMES_PAYLOAD` | 2663 | 0s |
| `sab_resend_envelope` | 2W 2G, content, seed orphan, MaxPuts 2, MaxResends 1, MaxVer 2, faults unbounded | red | red: `INV_I1_NO_RECURRENCE` | 25354 | 0s |
| `sab_resend_envelope_generation` | 2W 2G, generation, seed orphan, MaxPuts 2, MaxResends 1, MaxVer 2, faults unbounded | green: a resend lands a new token | cut at 43,587,461 states, no violation | 43,587,461 | 300s |
| `safe_p1_adopt_lost` | 2W 2G, content, seed lost, MaxPuts 1, MaxVer 3, late id, faults unbounded | green | green | 3856415 | 31s |
| `safe_p1_adopt_lost_generation` | 2W 2G, generation, seed lost, MaxPuts 1, MaxVer 3, late id, faults unbounded | green | green | 3856415 | 30s |
| `safe_p1_stale_list` | 2W 2G, content, seed pend, MaxPuts 1, MaxVer 3, late id, faults unbounded, StaleList | green | green | 17502063 | 135s |
| `safe_3writers_p1` | 3W 2G, content, seed pend, MaxPuts 1, MaxVer 3, faults unbounded | green | green | 26586577 | 332s |
| `safe_p1_late_create` | 2W 2G, content, seed orphan, MaxPuts 1, MaxVer 3, late ic, faults unbounded | green | cut at 33,790,620 states, no violation | 33,790,620 | 400s |
| `wit_two_read_writer_at_g8` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_TWO_READ_WRITER_AT_G8` | 46769 | 0s |
| `wit_g7_from_stand` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_G7_FROM_STAND` | 450037 | 3s |
| `wit_g7_from_pend` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_G7_FROM_PEND` | 32147 | 0s |
| `wit_g7_from_cand` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_G7_FROM_CAND` | 398315 | 2s |
| `wit_g8_no_head` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_G8_NO_HEAD` | 82950 | 1s |
| `wit_body_delete_second_round` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_BODY_DELETE_SECOND_ROUND` | 3300 | 0s |
| `wit_g4_mismatch_standing` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_G4_MISMATCH_STANDING` | 4710 | 0s |
| `wit_g9_cand` | 2W 2G, content, seed spared, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_G9_CAND` | 1219 | 0s |
| `wit_g9_stand` | 2W 2G, content, seed lost, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_G9_STAND` | 164 | 0s |
| `wit_g8_stale_listed` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded, StaleList | red | red: `WIT_G8_STALE_LISTED` | 8276 | 0s |
| `wit_standing_dropped` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_STANDING_DROPPED` | 34186 | 0s |
| `wit_reuser_adopts_u_over_old_intent` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded, Budget | red | red: `WIT_REUSER_ADOPTS_U_OVER_OLD_INTENT` | 1326 | 0s |
| `wit_two_read_adopt_committed` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_TWO_READ_ADOPT_COMMITTED` | 2490 | 0s |
| `wit_delete_misses` | 2W 2G, content, seed pend, MaxPuts 2, MaxVer 4, faults unbounded | red | red: `WIT_DELETE_MISSES` | 2909 | 0s |
| `wit_over_budget_cand` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, OverBudget | red | red: `WIT_OVER_BUDGET_CAND` | 244 | 0s |
| `wit_link` | 2W 2G, content, seed orphan, MaxPuts 2, MaxVer 4, faults unbounded, Linker | red | red: `WIT_LINK` | 178 | 0s |
| `live_base` | 2W 1G, content, seed orphan, MaxPuts 2, MaxVer unbounded | green | green | 254988 | 6s |
| `live_pend` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded | green | green | 251822 | 6s |
| `live_lost` | 2W 1G, content, seed lost, MaxPuts 2, MaxVer unbounded | green | green | 169084 | 5s |
| `live_spared` | 2W 1G, content, seed spared, MaxPuts 2, MaxVer unbounded | green | green | 251824 | 6s |
| `live_rebuild` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded, Rebuild | green | green | 789322 | 18s |
| `live_orphans2` | 2W 1G, content, seed orphans2, MaxPuts 2, MaxVer unbounded | green | green | 533428 | 12s |
| `live_fault1` | 2W 1G, content, seed orphan, MaxPuts 2, MaxVer unbounded, faults 1 | green | green | 867487 | 21s |
| `live_pend_fault1` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded, faults 1 | green | green | 855933 | 69s |
| `live_spared_fault1` | 2W 1G, content, seed spared, MaxPuts 2, MaxVer unbounded, faults 1 | green | green | 855937 | 20s |
| `live_orphans2_fault1` | 2W 1G, content, seed orphans2, MaxPuts 2, MaxVer unbounded, faults 1 | green | green | 1819041 | 42s |
| `live_late_fault1` | 2W 1G, content, seed orphan, MaxPuts 2, MaxVer unbounded, late ic/id/bd, faults 1 | green | green | 2317962 | 59s |
| `live_pend_late_fault1` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded, late ic/id/bd, faults 1 | green | green | 2224778 | 54s |
| `live_late_put` | 2W 1G, content, seed orphan, MaxPuts 2, MaxVer unbounded, late put | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 143118 | 3s |
| `live_sab_g9_before_g2` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded, Sab g9_before_g2 | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 176999 | 3s |
| `live_sab_g9_before_g2_orphans2` | 2W 1G, content, seed orphans2, MaxPuts 2, MaxVer unbounded, Sab g9_before_g2 | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 164985 | 3s |
| `live_sab_precut_head` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded, Sab precut_head | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 170138 | 3s |
| `live_sab_precut_g2_only` | 2W 1G, content, seed orphan, MaxPuts 2, MaxVer unbounded, Sab precut_g2_only | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 552011 | 10s |
| `live_sab_no_touched_head` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded, Sab no_touched_head | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 166417 | 3s |
| `live_sab_g8_before_g7` | 2W 1G, content, seed pend, MaxPuts 2, MaxVer unbounded, Sab g8_before_g7 | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 175084 | 3s |
| `live_sab_g8_before_g7_base` | 2W 1G, content, seed orphan, MaxPuts 2, MaxVer unbounded, Sab g8_before_g7 | red | red: `LIVE_UNREFERENCED_BODY_DELETED` | 160193 | 3s |
| `live_sab_no_g9` | 2W 1G, content, seed lost, MaxPuts 2, MaxVer unbounded, Sab no_g9 | red | red: `LIVE_NO_INTENT_LEFT` | 53512 | 1s |

## Reduced red traces {#traces}

**`live_sab_g9_before_g2`** (G9 may adopt in the round whose fold condemns):
1. Seed `pending(1)`, `I(B,1)`, body 1. G4 deletes body 1 and commits `standing(1)`; a writer added its edge after
   that round's cut.
2. The writer sees no body and publishes body 2, which lands.
3. The next cut holds the edge: G6 drops `standing(1)`. `I(B,1)` stays. The writer abandons.
4. The next round folds the transition. G9 adopts `I(B,1)` in place of G2: `HEAD` shows 2, so `standing(1)`.
5. The next round is untouched: G8 removes `I(B,1)`. No row, no transition: body 2 stays.

**`live_sab_precut_head`** (a decision `HEAD` read before the cut):
1. GC deletes body 1 and commits `standing(1)`.
2. A round reads `HEAD B` early: absent. A writer publishes body 2 and abandons.
3. The cut shows zero and a touched key; the early answer hides 2, so G8 removes `I(B,1)`. Body 2 stays.

**`live_sab_no_touched_head`**, **`live_sab_g8_before_g7`**: the round after `standing(1)` meets body 2 on a touched
key. Without the supersede `HEAD`, or with G8 first, G8 removes `I(B,1)` and the round folds the last
transition. Body 2 stays.

The `sab_*` traces are the schedules S1 to S7 and S9 of §4.3 of the specification.

## Not modelled {#not-modelled}

- Incomplete cuts and suppressed destruction: every cut is complete (A7).
- `cas-fsck`: by F2 and F5 it deletes no intent and no body.
- An empty real round consuming a touched window: a model round commits only when it changes the row.
- Several keys, shards, batching across rows, read-ahead, a token that does not canonicalise, token spelling,
  malformed intent bytes, multipart transport, the migration (`CaBlobIntentMigration.tla`).

## Runs not made {#not-run}

- `safe_rebuild` (content, `MaxPuts = 2`): stopped by hand at 19,611,237 distinct states, no violation.
- Cut, no violation, and not re-run with smaller bounds: `safe_base`, `safe_pend`, `safe_late_intent_delete`,
  `safe_late_intent_delete_generation`, `safe_rebuild_generation` (900 s, `MaxPuts = 2`; their `MaxPuts = 1`
  forms are green); `safe_pub_integrity`, `safe_pub_integrity_generation` (2 GC actors; green with 1);
  `safe_p1_late_create`.
- Not run: `safe_p1_late_create_generation`, `safe_p1_stale_list_generation`, `safe_3writers_p1_generation` (with
  `MaxPuts = 1` both dialects give the same state graph: `safe_p1_adopt_lost` and its generation form match state
  for state). `safe_late_create`, `safe_late_create_generation`, `safe_late_body_delete`, `safe_late_body_delete_generation`, `safe_adopt_lost`, `safe_adopt_lost_generation`, `safe_adopt_spared`, `safe_adopt_spared_generation`, `safe_lost_commit`, `safe_lost_commit_generation`, `safe_stale_list`, `safe_3writers`, `safe_over_budget`, `safe_linker`, `safe_budget`, `safe_maxver4_base`, `safe_maxver4_base_generation`, `safe_maxver4_pend`, `safe_maxver4_pend_generation`, `safe_maxver4_late_intent_delete`, `safe_maxver4_late_intent_delete_generation`, `safe_v2_late_intent_delete`, `live_pend_late_fault2`, `live_ctl_keep_standing_on_spare`, `live_ctl_g9_at_indegree`, `ctl_keep_standing_on_spare`, `ctl_g9_at_indegree`.
