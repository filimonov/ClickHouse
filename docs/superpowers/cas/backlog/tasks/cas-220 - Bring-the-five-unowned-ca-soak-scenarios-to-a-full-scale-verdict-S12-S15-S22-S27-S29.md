---
id: CAS-220
title: >-
  Bring the five unowned ca-soak scenarios to a full-scale verdict (S12, S15,
  S22, S27, S29)
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:20'
labels:
  - 'area:soak'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'needs:measurement'
  - 'origin:soak'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - utils/ca-soak/scenarios/RUN_HISTORY.md
  - utils/ca-soak/scenarios/cards/s15_s18_shards_lifecycle.py
  - utils/ca-soak/scenarios/cards/s28_s33_corner.py
priority: medium
type: task
ordinal: 278000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`utils/ca-soak/scenarios/RUN_HISTORY.md` has `full`-scale rows for 20 scenarios. Unresolved and not owned by another task:
S15 is inconclusive on a card bug (`:596`, `Code: 41 Cannot read DateTime: unexpected number of decimal`); S29 is inconclusive (`:347`, `:599`);
S12, S22 and S27 are green at `ci` scale but have never run at `full`.
Owned elsewhere: S03 (`CAS-152`), S05 (CAS-99), S23 (CAS-97), S01 (CAS-100), S07 (CAS-104), S10 (CAS-98), S11 (CAS-106).

Provenance: BACKLOG/testing-and-ci.md [ci-full-scale-sweep]; verified 2026-09-26 against 8b87aa15d21. The 'RSS-attribution / manifest-cap measurement doc' sub-point is carried by CAS-100 and CAS-107.

Update 2026-11-21: Remaining scope after the 2026-11-19 runs: only the five full-scale runs are missing (S15 DateTime card bug no longer reproduces: ci/dev passes at b34ad0c95972 and b92bd24789c9; S29 passed at ci scale 2026-11-19 04:50). The :596/:599 pointers are 2026-08-31 rows, not current blockers.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every subtask is done and each of the five scenarios has a `full`-scale row in `RUN_HISTORY.md` with pass or fail
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
First recorded: 2026-09-26 (5245fbc7a76, by 'ci-full-scale-sweep')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: rewrite (high). Evidence: utils/ca-soak/scenarios/RUN_HISTORY.md: still no passing full-scale row for any of S12, S15, S22, S27, S29 (last full rows S15 line 596 and S29 line 599 are inconclusive). But S15 passes at ci scale since 2026-08-31 (line 601, 717) and S29 passed at ci scale 2026-11-19 (line 715), so the stated blockers (S15 DateTime card bug, S29 inconclusive) are partly resolved.. Changes: impact:operability; description: Update paragraph appended. Links: Children CAS-220.1 to CAS-220.5 carry the five runs.. Pointers stale: RUN_HISTORY.md :596 and :599 cited as open blockers are superseded by later ci passes (lines 601, 715)..
---
<!-- COMMENTS:END -->
