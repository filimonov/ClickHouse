---
id: CAS-148
title: >-
  Log the snapshot publisher's back-off on a `Writing` ref lane at Debug, and
  name the lane state
status: Done
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-17 20:01'
labels:
  - 'area:observability'
  - 'area:ref-ledger'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
milestone: m-7
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
  - src/Disks/tests/gtest_cas_ref_chunked_flush.cpp
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f28
priority: high
type: chore
ordinal: 190000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The only recurring CAS warning on otel.demo, 113 in a week across 19 tables, is
"refusing snapshot publication while the append lane is not Ready (state 1)"
(`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp:4488-4493`). State 1 is
`RefLaneState::Writing` (`Pool/CasRefLedger.h:50-58`): an ordinary race with the writer. The publisher backs off and
retries (`CASRefSnapshotPublishBackoff` equals the warning count), which is 0.8% of 13.6k publications. It logs at Warning
with no rate limit and prints the state as a number (audit F28).

Provenance: BACKLOG/gc.md#otel-demo-s3-budget-audit-2026-09-25 (F28 second half). `gtest_cas_ref_chunked_flush.cpp:1072` quotes the message; keep that test's wait working. Verified 2026-09-26 against d4be7f7045a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A `Writing` lane logs the back-off at Debug; `Wedged`, `NeedsRecovery`, `Closed` and `Faulted` keep Warning
- [ ] #2 The message prints the state name, not its integer
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
First recorded: 2026-09-26 (83949645ce7, by 'audit F28')

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commit b26ff0e9d66 "Name the append-lane state in the snapshot publication refusal" covers: #1 a `Writing` lane logs at `Debug`, every other state stays at `Warning`; #2 the message prints the name from `refLaneStateName`; `CASRefSnapshotPublishOrdering.LaneStateNamesCoverEveryState` checks six distinct names. Still open: none. The level split has no unit seam and was checked by reading. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Delivered in PR 2831 by `b26ff0e9d66`.
<!-- SECTION:NOTES:END -->
