---
id: CAS-104
title: >-
  Re-run S07's 20,000-column `OPTIMIZE FINAL` at full scale and attribute its S3
  connections
status: To Do
assignee: []
created_date: '2026-09-26 07:23'
updated_date: '2026-09-26 07:41'
labels:
  - 'area:read-path'
  - 'area:soak'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:soak'
dependencies:
  - CAS-74
  - CAS-75
references:
  - utils/ca-soak/scenarios/cards/s06_s08_manifest_parts.py
priority: medium
type: research
ordinal: 142000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
S07's 20,000-column `OPTIMIZE FINAL` stalled in an S3 retry storm caused by ephemeral-port exhaustion. Requests and connections scale with the column count.
No full-scale S07 run has happened since (`utils/ca-soak/scenarios/RUN_HISTORY.md`, last S07 rows dev/ci on 2026-08-31 and 2026-09-01).
Candidate causes are open in u05: 100-request keep-alive rotation and connection resets from over-long read ranges. The 2026-09-25 audit did not cover this workload shape.

Provenance: BACKLOG/performance.md#scale-findings [wide-part O(columns)] (+ orphaned 2026-08-04 triage duplicate). Verified 2026-09-26 against dd0ed2f263a.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 S07 at full scale completes or fails on HEAD with `DiskConnectionsCreated`, TIME_WAIT count and requests per column recorded
- [ ] #2 If it still exhausts ports, the dominant connection source is named and a fix task is filed
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
Merged from mounts-and-lifecycle.md (#2243 housekeeping, S07 re-rated): the same port-exhaustion condition took the mount lease down and discarded in-flight PartWriteTxns, so S07 is availability class, not cost only.
<!-- SECTION:NOTES:END -->
