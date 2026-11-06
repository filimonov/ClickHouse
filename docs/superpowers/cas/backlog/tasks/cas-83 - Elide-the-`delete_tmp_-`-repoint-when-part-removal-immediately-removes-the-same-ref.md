---
id: CAS-83
title: >-
  Elide the `delete_tmp_*` repoint when part removal immediately removes the
  same ref
status: In Progress
assignee: []
created_date: '2026-07-15'
updated_date: '2026-11-06 23:12'
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
  - >-
    docs/superpowers/reports/2026-09-30-cas-part-removal-flow/01-removal-flow-map.md
  - >-
    docs/superpowers/reports/2026-09-30-cas-part-removal-flow/02-design-inputs.md
  - docs/superpowers/reports/2026-09-30-cas-part-removal-flow/03-yardstick-ab.md
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
2026-09-30 PR opened: https://github.com/Altinity/ClickHouse/pull/2452 (base antalya-26.6, head fix/antalya-26.6/cas-part-removal-one-drop-pr = fd7be6776a7). Diff: one 11-line hunk in `DataPartStorageOnDiskBase::remove`, test 05054, two docs files. Comment-only edits in CAS files were dropped at the owner's request (isolated upstream impact). Local gates on the same compiled code: gtest CAS* 2556/0, 49 stateless, 05054 x10. Next: CI on the PR.

2026-10-22, on `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8): a same-namespace rename of a committed part (`delete_tmp_*` included) is one ref-log `Move` with the manifest unchanged: no manifest PUT, no `_ckpt` write from the republish, one append + the later drop. The elision target on 26.8 is therefore one Move append per removal, not a repoint; re-measure on otel.demo after the 26.8 rollout before sizing the remaining work.

2026-11-07 audit: related merges: https://github.com/Altinity/ClickHouse/pull/2452 (41824f955e7, antalya-26.6, 2026-10-01), https://github.com/Altinity/ClickHouse/pull/2579 (003a998407b) and https://github.com/Altinity/ClickHouse/pull/2647 (d8d64d936e2) on antalya-26.8. Remaining: re-measure after the 26.8 rollout.
<!-- SECTION:NOTES:END -->
