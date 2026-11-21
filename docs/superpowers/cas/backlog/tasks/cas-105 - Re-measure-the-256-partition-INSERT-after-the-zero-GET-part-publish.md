---
id: CAS-105
title: Re-measure the 256-partition INSERT after the zero-GET part publish
status: To Do
assignee: []
created_date: '2026-07-06'
updated_date: '2026-11-21 08:20'
labels:
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:soak'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-2
dependencies:
  - CAS-176
references:
  - src/Storages/MergeTree/ReplicatedMergeTreeSink.cpp
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
priority: low
type: research
ordinal: 143000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A 256-partition INSERT took ~10 s (~40 ms per part), because each partition's part is published serially.
Concurrent per-partition commit (stage 2) is postponed by user decision (`DRAFT-2`); the zero-GET publish (decision-5, audit F31) cuts per-part latency and is scheduled.
Measure what is left after it before reopening stage 2.

Provenance: BACKLOG/performance.md#scale-findings [partitioned-INSERT O(partitions)]. Verified 2026-09-26 against dd0ed2f263a.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Wall time and per-part publish latency of a 256-partition INSERT are recorded before and after the zero-GET publish
- [ ] #2 The result is attached to the stage-2 draft as its go/no-go input
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
First recorded: 2026-07-06 (4abf5b743ed, by 'partitioned-INSERT O')

Merged from u19b-soak (merge into CAS-105): From the soak ledger's partitioned-INSERT entry (2026-07-06, S11 full, 256 buckets): 4-5 INSERTs per minute. Fix direction recorded then: when the parts of one INSERT share a namespace, publish them in one combined flush; check whether the ref-lane combine window (CAS-287) already covers multi-partition INSERTs before reopening stage 2.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: the prerequisite landed (PR 2831, 0aa837e942e, ancestor of the checked branch; CAS-176 notes: no catalog/_ckpt/manifest read on a lone INSERT); no 256-partition measurement is recorded in the task or in docs/superpowers/reports. Changes: none. Links: related CAS-287 (ref-lane combine window); prerequisite CAS-176 is partly delivered, its 'no GET' part is merged. Pointers stale: AC1 asks for a 'before' number; the pre-2831 baseline must come from the 10 s / 40 ms-per-part figure in the description. Would settle it: the before/after wall time and per-part publish latency of a 256-partition INSERT on the post-2831 code (nothing recorded yet).
---
<!-- COMMENTS:END -->
