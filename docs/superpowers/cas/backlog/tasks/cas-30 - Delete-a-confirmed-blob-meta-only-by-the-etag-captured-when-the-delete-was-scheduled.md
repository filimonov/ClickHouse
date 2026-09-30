---
id: CAS-30
title: >-
  Delete a confirmed blob meta only by the etag captured when the delete was
  scheduled
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-09-30 20:29'
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
2026-09-30 OWNER DECISION (final shape of PR1): G1 - remove the in-process confirmation registry, graduation always reads the marker (revert of the request-saving optimization) - plus spec r1's round checks (C1 in the delete job, C3 at the gate) and the re-stamp (C2). No pins, no format change. Spec r3 = r1 without the registry, with r2's schedules, model (returning leader, marker-preservation invariants) and tests, and codex r2's two MAJORs (budget check before the gate's read; rebuild residual stated accurately). V1/V2r/V3 alternatives recorded in CAS-329.
<!-- SECTION:NOTES:END -->
