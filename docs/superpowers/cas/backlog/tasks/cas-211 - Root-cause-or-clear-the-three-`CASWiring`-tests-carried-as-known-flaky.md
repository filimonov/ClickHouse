---
id: CAS-211
title: Root-cause or clear the three `CASWiring` tests carried as 'known flaky'
status: To Do
assignee: []
created_date: '2026-09-04'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:testing'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:repro'
  - 'origin:review'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - src/Disks/tests/gtest_ca_wiring.cpp
priority: low
type: research
ordinal: 268000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CASWiringOps.FreezeViaHardLinksIntoShadow`, `CASWiringGc.DroppedPartIsReclaimedByRounds` and `CASWiringGc.DisplacedTreeBlobsReclaimedThroughRealPath`
(`T/gtest_ca_wiring.cpp:1440`, `:1522`, `:1579`) were once listed as known pre-existing failures and carried through the suite rename without a root cause.
They passed the strict gate of 2026-08-24. A known red without an RCA is not allowed; either it is gone or it has a cause.

Provenance: BACKLOG/testing-and-ci.md [ca-s3-stateless-lane] (the '3 pre-existing gtest failures'); names from the item's original text; verified 2026-09-26 against 8b87aa15d21.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The three tests pass 50 repeated runs under Debug and ASan, or each failure has a named cause and a fix task
- [ ] #2 No document lists them as known failures any more
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
First recorded: 2026-09-04 (5d9bb3ec707, by 'ca-s3-stateless-lane')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: The three tests still exist in cas-pr1 src/Disks/tests/gtest_ca_wiring.cpp (:1579, :1661, :1718); no repeated-run or RCA record is in the task, which itself says they passed the strict gate of 2026-08-24.. Changes: impact:operability. Pointers stale: gtest_ca_wiring.cpp :1440/:1522/:1579 are now :1579/:1661/:1718.. Would settle it: A 50-run Debug and ASan result for the three tests, or an RCA..
---
<!-- COMMENTS:END -->
