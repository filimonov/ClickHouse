---
id: CAS-358
title: >-
  Stop the orphan sweep from replaying a namespace without a `_snap` from
  genesis on every page
status: To Do
assignee: []
created_date: '2026-10-12 09:50'
updated_date: '2026-10-14 17:26'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:review'
milestone: m-1
dependencies:
  - CAS-391
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2542'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.cpp
priority: medium
type: research
ordinal: 452000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
**Decision (2026-10-14): two stages, no on-store state, no fold or writer change.** Spec: `docs/superpowers/specs/2026-10-14-cas-orphan-sweep-cadence-and-snapshot-view-design.md`. The owned-manifest-set design (`2026-10-13-cas-orphan-sweep-owned-manifest-set-design.md`, rev.8) is withdrawn: its backfill of pre-existing lives is a second reference-tracking machine for a janitor that reclaims ~1 000 bodies a month.

1. **Cadence (ship first).** The sweep runs as a circuit (pages from an empty cursor until the LIST wraps) at most once per `manifest_sweep_interval_sec` (default 86 400); between circuits a round does no LIST and no reads. Clock in leader memory (`mono_ms_fn`), reset on lease acquire, so a new leader starts one circuit at once. Per-page behaviour unchanged. ~20 lines in `Gc/CasGc.cpp` plus one setting.
2. **Snapshot view (later).** The owner view of a catalog-named life is the checkpoint-named snapshot read through `readCheckpointSnapshotBase` (at most four GETs per namespace per page). Delete iff: floor-eligible (CAS-36), row not `Creating`, `P` exists and `K ∉ snap(P)`, build epoch `E < P.writer_epoch`, coverage clean and fold cursor `C ≥ P`. Otherwise retain under a counted class (`NoSnapshot`, `InSnapshot`, `SnapshotEpoch`, `CursorBelowSnapshot`). Keys with `E < _ckpt.life_epoch` (an older life of the same name) and rowless keys use the floor alone. `activeManifestKeys`, the committed-tail walk and `gc_round_sweep_recovery_op_budget` are deleted.

### Problem

`activeManifestKeys` (`CasOrphanManifestSweep.cpp:192`) replays the table from `_snap` or genesis (`CasRefCkpt.cpp:176-180`) on every page that nominates a candidate in the namespace, then walks the committed tail a second time. A `_snap` exists only above 256 logs or 1 MiB (`CasPool.h:290-291`); every remount adds a seal-only epoch.

| metric | PR 2542 stand | otel.demo (7 days to 2026-10-13) |
|---|---|---|
| streams / with a `_snap` | 792 / 74 | — |
| seal-only epochs | 3 876 | — |
| sweep ref-log GETs per day | ~956 k | ~0.98 M (0.29 M taken, 0.69 M read-ahead wasted) |
| bodies deleted by the sweep | — | 0 in the week, 1 077 since 2026-09-14 |

### Rejected

- Fold-maintained owned-manifest set (spec rev.8): backfill complexity, five codex rounds each found a new hole.
- Blob in-degree as the owner set: sharded by blob hash; inline-only manifests have no edge rows.
- `_snap` threshold counting seal-only epochs: shortens the replay, keeps it.
- Circuit stamp in `gc/state`: format frozen since 26.6.4.
- `LIST _snap/` as the source: a `_snap` whose `_ckpt` CAS did not land is not a base.

### Related

- CAS-36: floor before the catalog cut; unchanged, step one of the page.
- CAS-359: `prev_epoch_seal` hint bound; still useful for writer recovery and the fold, the sweep no longer hints.
- CAS-387: one sequence per life; orthogonal.
- CAS-386: prune vs adopted seal; orthogonal.
- CAS-87, CAS-86, CAS-310: superseded or absorbed by stage 2 (the tail walk is deleted; the CAS-310 invariant is a stage-2 TLA property).
- Optional follow-up, not needed for correctness: writer publishes a `_snap` after a recovery that found a non-empty tail (`admitSnapshotPublishUnderStateLock`, `CasRefLedger.cpp:4140`).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Stage 1: setting manifest_sweep_interval_sec (default 86400, 0 = circuits back to back); a round with an empty cursor before the deadline does no LIST and no reads and reports idle=1; a circuit in progress continues regardless of the clock; new leader starts a circuit at once; tests S1-S5 of spec §3.5 green
- [ ] #2 Stage 1 measured on otel.demo: sweep GETs per day and 404s per day before and after, in a report under docs/superpowers/reports/
- [ ] #3 Stage 2: the owner view is readCheckpointSnapshotBase only; activeManifestKeys, the committed-tail walk, read-ahead hints in the sweep and gc_round_sweep_recovery_op_budget are deleted; no _log GET outside {P, predecessor seal} (T7)
- [ ] #4 Stage 2: manifestDeletionPremise implements the five rules of spec §4.1 with retain classes NoSnapshot, InSnapshot, SnapshotEpoch, CursorBelowSnapshot; older-life keys (E < life_epoch) and rowless keys use the floor alone; T1-T13 of spec §4.7 green; sweepNamespace and the cursor page agree (T12)
- [ ] #5 Stage 2: floor sampled before the catalog cut (CAS-36) unchanged
- [ ] #6 TLA: CaGcRootLocalPartManifestCore gets snapPos and the snapshot-based GSwView; sab_cursorbelowsnap reproduces the CAS-310 trace and sab_snapepoch deletes a live staged build; the fix configs stay green
- [ ] #7 Docs: garbage-collection.md subsection on orphan manifests (circuit, five rules, retain classes, no-snapshot case, settings) and the manifests-and-refs.md flow note; the withdrawn spec carries a status line pointing at the new one
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
Decision log (times are commit times, local):

- 2026-10-12 11:50 (13d8fe980e7): opened from the review of PR 2542; contributor asked which direction we prefer.
- 2026-10-12 14:46 (18d614a00bd): blob in-degree evaluated and rejected; owned-manifest set from the fold preferred.
- 2026-10-12 14:55 (e1864a2b168): owner decision, no replay at all; `_snap` threshold is not the fix. PR 2542 later decided not merged (CAS-359).
- 2026-10-12 15:09 (4822008176b): owner constraint, forward compatibility with no operator action; rules 1-5 in Description.
- 2026-10-12 (7909f9d2bb7, CAS-305): `as_of` is the existing `RefTxnId` pair; finality still comes from seals until CAS-305 (d).

2026-10-12: overlaps CAS-36 (same function, `planManifestCursorPage`). The CAS-36 fix proves safety by observation order floor → catalog cut → owner view; here the owner view is older than the round (fold set `as_of` the cursor), so a build that ended after the cursor but before the floor read is visible only in the tail read, which must follow the floor sample. Rule 4 would retain rowless namespaces forever because the fold has no set for a namespace without a row. Three ACs added; depends on CAS-36.

- 2026-10-13 (e6d1e390336): design spec docs/superpowers/specs/2026-10-13-cas-orphan-sweep-owned-manifest-set-design.md, rev.1. Sweep reads no ref log; owned set per gc-shard keyed (life_id, ManifestRef); seed exactly at the cursor, deferred while _snap is above it, no as_of state; owned seal is a side object, fold seal bytes unchanged.

- 2026-10-13: backlog cross-check. Closes CAS-87; absorbs CAS-86 and CAS-310 (deps set on them); notes left on CAS-315, CAS-46, CAS-29.6, CAS-29.10 (spec §9). Unchanged by this design: CAS-36, CAS-357 (floor stays step 1), CAS-37, CAS-91, CAS-139, CAS-311, CAS-29.12, CAS-338.

- 2026-10-13: spec reviewed by codex gpt-6-astra high, five rounds (tmp/cas358/codex_spec_review_1..5.md): 8/6/4/5/3 findings, verdict on rev.6 APPROVE WITH MINORS, minors applied in rev.7. Main corrections along the way: ownership deltas independent of bodies; candidate-first two-pass sweep; seed exact at C with T >= C precondition, endpoint check, admission-to-completion budget, zero = unlimited, fairness cursor seed_after; delta admission whitelist; empty-run initialization; HEAD/GET-404 re-evaluation; historical foldOwned and cursor-epoch gate in the model; keep=1 adopted-generation prune split out as CAS-386.

- 2026-10-13: owner question 'can the sweep rely on the latest _snap alone?'. Yes with one guard: delete only if below floor, absent from snap(P), build epoch < P.writer_epoch, and the fold cursor C >= P (every -1 at or below P folded). No on-store state, no fold change; sweep reads _ckpt and _snap, no logs. Gap: a _snap exists only when the tail exceeds 256 logs or 1 MiB (CasPool.h:290); fix on the writer side by publishing after recovery whenever the tail is non-empty (admitSnapshotPublishUnderStateLock, CasRefLedger.cpp:4140), which also shortens table load. The owned-set design (spec rev.8) is superseded by this if the owner confirms; CAS-387 (epoch out of the log key) is orthogonal and does not remove the second read by itself.

- 2026-10-14: owner decision after the complexity pushback on rev.8: stage 1 cadence (circuit per interval, default a day), stage 2 snapshot-only owner view with the C >= P and E < P.writer_epoch guards. Spec docs/superpowers/specs/2026-10-14-cas-orphan-sweep-cadence-and-snapshot-view-design.md rev.1; rev.8 withdrawn. Rebirth under the same name needs no extra rule: creation over a Removing row waits for the terminal fold and row removal (CasRefLedger.cpp:1349), so older-life keys are decided by the floor alone.

- 2026-10-14: codex gpt-6-astra high round 6 on rev.1 (tmp/cas358/codex_spec_review_6.md): REVISE, 2 critical / 4 major / 4 minor. Critical 1: §4.4 listed the catalog cut before the floor (contradicting the CAS-36 order the spec depends on); fixed. Critical 2: nothing in the writer enforces that an AddPrecommit names a manifest of its own epoch (requireAlive checks the epoch, the runtime is captured later; a self-remount in between mints an epoch-4 grant for an epoch-3 build); the same assumption underlies today's rule (1) and the floor; split out as CAS-391 and made a stage-2 prerequisite. Majors: CAS-362/363 restored as prerequisites; §4.6 mixed-leader claim narrowed to format compatibility (26.6.4 without CAS-36 is unsafe on its own); TLA sketch given historical ownership, committed-removal property and the right sab_snapepoch witness; gc_round_sweep_recovery_op_budget stays declared as a no-op. Minors: CoverageClass names, clamp wording, rebirth wording and T9 target, lease-term vs renewal reset, cost wording. rev.2 adds S6-S8 and T14-T17.
<!-- SECTION:NOTES:END -->
