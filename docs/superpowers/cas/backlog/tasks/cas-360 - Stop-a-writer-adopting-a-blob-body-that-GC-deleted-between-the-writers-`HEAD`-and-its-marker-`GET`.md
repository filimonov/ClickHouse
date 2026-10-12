---
id: CAS-360
title: >-
  Stop a writer adopting a blob body that GC deleted between the writer's `HEAD`
  and its marker `GET`
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-10-12 18:09'
labels:
  - 'area:gc'
  - 'area:write-path'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMetaWriter.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/meta.md
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/blob.md
priority: critical
type: bug
ordinal: 454000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`PartWriteTxn::ensureBlobPresent` (`Pool/CasPartWriteTxn.cpp`) reads the body with `HEAD`, then the `.meta` marker with a separate `GET`. When the marker is absent it backfills `Clean` and returns `Observed`, with no second `HEAD`. GC (`Gc/CasGc.cpp:performRedeleteIo`, `applyRedeleteOutcome`) deletes the body by exact token and immediately schedules `scheduleConfirmedMetaDelete` in the same pass.

Schedule: blob is `delete_pending`; writer W precommits after the delete pass's cut (so `settleEntry` does not see its edge), W's `HEAD` lands before the body `DELETE`, GC deletes the body and then the marker, W's `GET` finds no marker, adopts the deleted body. Dangling ref; no pause needed, only a `GET` a few RTT late (first-attempt timeout resend, load, thread preemption). Verified in code by three independent codex runs (docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-blob.md, w1-reflog.md, w1-catalog.md); through relink it also breaks `ConfirmedRelinkNeverDangles`.

Models missed it because `CaGcCondemnMarkerGate:WAdopt` and `CaBlobPublishCore:ObserveMeta` read body presence and the marker in one step.

Fix (no format change): delete the marker one round after the body. Round R deletes the body and carries the entry as `delete_pending`; round R+1 settles it against fresh in-degree: `> 0` spares it (marker stays `Condemned`, the writer republishes), `= 0` does `HEAD` (expect `Gone`), deletes the marker synchronously and only then drops the entry. Clock-free: W's precommit precedes W's `HEAD`, which precedes the body delete, which precedes R+1's cut, so W's edge is in R+1's in-degree. Also closes the orphan-marker leak (failed advisory marker delete drops the entry today) and the false fsck `meta_without_body` ERROR.

Alternative considered: re-`HEAD` before adopting when the marker is absent (5–10 lines); relies on ETag uniqueness, which SDK resends break.

Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/meta.md (S9), docs/superpowers/cas/2026-10-12-late-request-audit/objects/blob.md (S13), docs/superpowers/cas/2026-10-12-late-request-audit/objects/SUMMARY.md#orphan-meta. Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `CaGcCondemnMarkerGate` splits `WAdopt` into `WHead`/`WMetaRead`; TLC shows the violation with the same-pass marker delete and holds with the deferred delete
- [ ] #2 gtest with a hook between the writer's `HEAD` and marker `GET`: GC deletes body and marker; the writer republishes instead of adopting
- [ ] #3 a failed marker delete is retried next round (entry dropped only after the marker delete is confirmed); fsck reports `meta_without_body` as a warning
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-12 model result (`06e331ada7c`, `docs/superpowers/models/CaGcCondemnMarkerGate_RESULTS.md#cas-360`): `WAdopt` split into `WHead`/`WMetaRead`/`WReHead`/`WBackfill` under `SplitAdopt`. Current protocol violates `NoDangle` (`cur_headget`, 15 states; also `cur_headget_all`, `cur_deposed`). All three candidates are green with load-bearing sabotages: A deferred marker delete (`fix_deferA*`, up to 38.4 M states), B re-HEAD on absent marker (`fix_reheadB*`, up to 50.9 M), C republish on absent marker (`fix_republishC*`).

A needs more than one round of delay: the marker delete must run in a round strictly after a pass that saw the body `Removed`/`Gone` and committed; deleting in the round that first sees `Gone` reopens the race because a deposed attempt can delete the body after that round's cut (`sab_deferA_gonenow`). So A needs a persisted 'body gone' mark on the entry (frozen run format; candidate: the unused `(delete_pending=1, marker_confirmed=0)` combination) and the redelete outcome must reach the run being written (`rmr.redelete` entries leave the ledger before the I/O; the seal is written in phase 10, deletes run in phase 11). B is ~10 lines in the `!loaded` branch (second HEAD before the backfill) but relies on tokens not recurring, i.e. on CAS-361. C costs a full upload per absent marker.

Separate leak found (`cur_orphan_retry`): the gate's retry in `confirm_condemned_marker` stamps the current round, later than `entry.condemn_round`, so `deleteConfirmedMeta(ref, entry.condemn_round)` skips it forever. Fix: pass the marker round the gate observed, or under A delete any `Condemned` marker below the current round.

2026-10-12 decision after the two-model consult: fix = C. Only a `Clean` marker read after the body `HEAD` permits adoption; a present body with an absent marker is republished exactly like an absent body (`reason = Absent`, marker created by the reconcile). `ensureBlobPresent`: adopt condition `loaded && state == Clean`, the `putMetaIfAbsent` backfill block removed (about -12 lines), event text and `CASMetaAdoptBackfill` repurposed or replaced. The mandatory `HEAD` stays (HEAD-before-PUT; a `Clean` marker does not prove the body exists, e.g. an old writer's backfill during a rolling upgrade; size check). C does not depend on CAS-361; B (re-HEAD) does. A rejected by both consultants: `(delete_pending=1, marker_confirmed=0)` parses in 26.6.4 but an old leader re-deletes and deletes the marker in the same pass, and `GcOutcomes` is capped (5000) and lossy, so it cannot carry the mark.

Cost measured on otel.demo (one node, 26.6.4, uptime 16.8 days): `CASMetaAdoptBackfill` = 8 vs `CASMetaCreateClean` = 2,820,124 and `CASBlobBodyPutAvoided` = 7,105, i.e. 8 extra uploads in 17 days. `CASGCRetiredSparedByReref` = 117 confirms writers do hit blobs GC already graduated.

Leak fix in the same change: `applyRedeleteOutcome` passes the deleting round - 1 as the bound, so `deleteConfirmedMeta` also removes a `Condemned` marker the graduation gate re-stamped at a later round (`cur_orphan_retry`); under C a marker delete is safety-neutral. Old writers stay exposed until upgraded.

2026-10-12 model of the chosen fix (`b483e4ea41c`, RESULTS `#cas-360-fix-c`; 61-row battery green): `fix_C_bound` (C + round-1 bound + all faults + deposed delete behind its own cut) green (MaxRounds 5); `fix_C_recur` green — C does not depend on CAS-361; `fix_B_recur` red — B does; `fix_C_orphan_retry` green and `sab_C_orphan_retry` red — the bound closes the leak; `mixed_old`/`mixed_new` red as expected (old writer's own adoption; new writer adopting on an old writer's backfilled `Clean`). A coarser model where a deposed attempt deletes at any time without its own in-degree is red for C and for today's bound alike (`fix_C_*_coarse`); checked against the code: the only source of redeletes is `settleEntry` → `rmr.redelete` (`CasBlobInDegree.cpp`) in the same attempt's fold, whose ref intake follows its `gc/state` read, so every deposed-attempt delete is behind its own cut — the coarse model is not reachable.
<!-- SECTION:NOTES:END -->
