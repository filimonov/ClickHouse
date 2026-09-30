---
id: CAS-83
title: >-
  Elide the `delete_tmp_*` repoint when part removal immediately removes the
  same ref
status: To Do
assignee: []
created_date: '2026-07-15'
updated_date: '2026-09-30 13:42'
labels:
  - 'area:write-path'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - 'theme:parity'
milestone: m-2
dependencies: []
references:
  - CA/ContentAddressedTransaction.cpp
  - CA/Parts/PartFolderAccess.cpp
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f2
  - docs/superpowers/cas/2026-09-16-msan-cas-s3-shard-budget-rca.md
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#open-questions
priority: high
type: enhancement
ordinal: 118000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Every part removal renames the part to `delete_tmp_*`, which republishes the committed ref (`CA/ContentAddressedTransaction.cpp:1459`,
`republishRef` in `moveDirectory`), and the next step deletes that ref. Each repoint costs a manifest PUT, a ref-log append,
a `_ckpt` write, then in GC two manifest GETs, one more manifest delete and one more log record.
otel.demo: 190,853 repoints/day, ~8% of PUTs, ~6% of GETs, a quarter of ref-lane mutations, a third of GC intake (audit F2).
The same-transaction supersede-clear (T8) already elides it for unlink+rmdir in one transaction; the cross-transaction removal flow does not.
The ref lane, not store latency, is the bottleneck (748 ms of an 887 ms insert is queue wait, audit conclusion 1), and on the
msan shard the per-part repoint grew from 0.83 s to 10.7 s median over four hours, so the elision removes queue load, not only requests.
`concurrent_part_removal_threshold_for_remote_disk = 1` is an unapplied stopgap, not the fix. Schedule next to GC stage A (spec 10.6).

Provenance: BACKLOG/gc.md [PART-REMOVAL-REPOINT] and #drop-path-head-of-line-and-repoint-ramp item 2 (ramp answered by audit conclusion 1: queue wait dominates). Roadmap §2 'Remove a part in one transaction' (now). Verified 2026-09-26 against 59494ebf366 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A part removal on a CAS disk emits no `delete_tmp_*` repoint: no manifest PUT, `_log` append or `_ckpt` write for the rename
- [ ] #2 Crash between rename and removal leaves a state recovery resolves to either the old part or no part, pinned by a test
- [ ] #3 On a soak or the stand, `CASRefRepoint` per day on `delete_tmp_*` refs drops to near zero and GC intake records per removal halve
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
First recorded: 2026-07-15 (db28579459a, by 'PART-REMOVAL-REPOINT')

2026-09-30, measured on the real stand. otel.demo (26.6.4.20001.altinityantalya), `system.cas_log` of both replicas, 6 hours before 2026-09-30 13:40 UTC: `manifest_put` 152,460; `build_publish` 152,457, of which 101,548 on `delete_tmp_*` refs; `ref_repoint` 50,774, all on `delete_tmp_*`; `ref_drop` 101,548, of which 50,774 on `delete_tmp_*`; `blob_put` 50,897. So only one manifest PUT in three is a real part publish; the removal flow writes the other two.
Trace of one part (`202609_491558_491558_0`, removal 13:21:49.57-13:21:50.82 UTC, 1.25 s in the ref lane):
1. `precommit` + `build_publish` of `delete_tmp_<part>` with a new manifest B (the rename);
2. `ref_drop` of `<part>` (manifest A);
3. `precommit` + `build_publish` + `ref_repoint` of `delete_tmp_<part>` to a new manifest C (the removal marks);
4. `ref_drop` of `delete_tmp_<part>` (manifest C).
Per removed part: 2 extra manifests, 4 ref-log transactions instead of 1, and for the GC 6 owner edges on 3 manifests instead of 2 edges on 1. With the flow reduced to one drop, manifest PUTs fall to a third, and the GC's manifest reads in `fold_ref_intake` (40.5% of round time on this stand, 4,645 bodies per round) fall by about two thirds. This removes the work that the manifest memo of CAS-29.12 would only cache.
<!-- SECTION:NOTES:END -->
