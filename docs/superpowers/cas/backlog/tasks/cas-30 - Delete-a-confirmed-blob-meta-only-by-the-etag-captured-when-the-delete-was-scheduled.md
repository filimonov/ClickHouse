---
id: CAS-30
title: >-
  Delete a confirmed blob meta only by the etag captured when the delete was
  scheduled
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-09-30 13:23'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:high'
  - 'touches:protocol'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-0
dependencies: []
references:
  - CA/Gc/CasGcMetaWriter.cpp
  - CA/Gc/CasGcMetaWriter.h
  - src/Disks/tests/gtest_cas_gc_meta_writer.cpp
  - docs/superpowers/reports/2026-09-30-cas-30-stale-meta-delete-adjudication.md
priority: critical
type: bug
ordinal: 37000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Data-loss class. `deleteConfirmedMeta` (`CA/Gc/CasGcMetaWriter.cpp:86-92`) re-reads the meta at execution time and
deletes by whatever etag it sees then; `GcMetaWriter::scheduleConfirmedMetaDelete` (`:160-167`) carries no etag.
Same code on altinity/antalya-26.6. Jobs deliberately outlive their `Gc` (they capture only `shared_ptr<State>`).
Interleaving: leader A deletes body t1 and queues its meta delete; a new leader condemns a fresh incarnation t2; A's
delayed job reads t2's `Condemned` meta and deletes it; a writer then sees an absent meta ("not condemned") and adopts t2
without rematerialization; the new leader's exact-token delete removes t2's body. A committed ref then names a deleted blob.
Fix: thread the etag observed when the delete was decided into `scheduleConfirmedMetaDelete` and call `deleteMetaExact`
with it; never delete by a fresh `loadMeta`. No on-S3 format change.

Provenance: BACKLOG/gc.md#gc-confirmed-meta-delete-etag-race (first raised as P1#2 of the 2026-08-05 codex stage-B review); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6, both unfixed.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A test that replaces the meta between scheduling and execution shows the delayed delete refused and the newer meta intact
- [ ] #2 No call path deletes a blob meta by an etag it read after the delete decision
- [ ] #3 The existing `CASGcMetaWriter` suites stay green under ASan and TSan
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
- [ ] #5 Death-test split applies if any new assertion is a `LOGICAL_ERROR`
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-09-26 (e35929424c6, by 'gc-confirmed-meta-delete-etag-race')

Second independent confirmation, 2026-09-30 (final review of `cas/gc-request-cuts` by codex gpt-6-astra, then an Opus adjudication): `docs/superpowers/reports/2026-09-30-cas-30-stale-meta-delete-adjudication.md`.
- Reachable on `antalya-26.6` @61d24118251 without any new code: the graduation gate trusts the in-process confirmation and never re-reads the marker, and the redelete does not read it either. Needs the leader frozen (heartbeat silent) with at least one queued meta-delete job, for about 2-3 minutes at the default 60 s interval (derived from the steal condition, not measured).
- No fence on the job's path: the request plane refuses only after pool teardown; `meta_pool_wait` drains before the `gc/state` CAS, so a deposed round's job still runs.
- Fix variant proposed there, different from the etag variant above: pass `RetiredEntry::condemn_round` to the job and delete only a `Condemned` meta whose `condemn_round` is not newer; and make `writeCondemnedMeta` re-stamp an already-`Condemned` meta of an older round, which closes a double-failure case where the old generation's marker object survives and ends up guarding the new one. Round numbers only increase. No format change, no extra request on the normal path. Compare with the captured-etag variant before choosing.
- The model `docs/superpowers/models/CaGcCondemnMarkerGate.tla` cannot see the schedule: `GSettleRedelete` deletes body and meta atomically. Split it and add an unfenced `GMetaDeleteJob` action.
- The safety argument of spec `2026-09-29-cas-gc-request-cuts-design.md` §2 (carried `marker_confirmed`) assumes the premise this defect breaks; that change must not ship before this fix.

2026-09-30, one more path to the stale job (`docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/05-memo-vs-deposed-leader-adjudication.md`): on antalya-26.6 an absent manifest body is an anomaly that sets `suppress_destructive` for the whole round, so a deposed leader whose successor deleted a body runs no deletes. With the manifest memo (CAS-29.12) a hit replaces the absent read, the round is not suppressed, and the deposed leader can reach up to `gc_round_redelete_budget` `deleteConfirmedMeta` jobs. The memo must not ship before this fix, or it needs a leadership re-check after the fold.
<!-- SECTION:NOTES:END -->
