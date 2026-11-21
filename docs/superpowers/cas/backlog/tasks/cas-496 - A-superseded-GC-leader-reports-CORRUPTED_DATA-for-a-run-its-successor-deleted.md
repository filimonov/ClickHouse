---
id: CAS-496
title: A superseded GC leader reports CORRUPTED_DATA for a run its successor deleted
status: To Do
assignee: []
created_date: '2026-11-16 14:29'
updated_date: '2026-11-21 08:26'
labels:
  - 'area:gc'
  - 'origin:soak'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: bug
ordinal: 601000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Soak 2026-11-16 (evidence master/tmp/partb/soak/run2): ch1 was GC leader, its round stalled under chaos; ch2 took the GC lease, committed three rounds and deleted the objects of generations 39 and 40; ch1, still inside its superseded round, read gc/gen/39/attempt/39/blob_target/0/0, got 404 and ended the round with Error, code 246 CORRUPTED_DATA 'CAS source-edge run: object ... is absent' (CA/Gc/CasBlobInDegree.cpp openSourceEdgeRun, fail-closed). Its next tick answered NotALeader. Nothing was committed, data is safe. Defect: the log says data corruption and <Error> 'CA GC round failed' where the leader only lost its lease. Wanted: before reporting an absent run as corruption, re-check the GC lease (the same rule as 'corrupt only under an unchanged _ckpt' in the ref chain reader) and end the round as fenced out. Code predates PR 2831.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:26
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 Gc/CasBlobInDegree.cpp:418-421 openSourceEdgeRun still throws CORRUPTED_DATA 'CAS source-edge run: object {} is absent' on a null stream with no lease re-check or fenced-out outcome. Changes: impact:operability. Links: same family as CAS-372 (S8 deposed-leader false CORRUPTED_DATA, other throw site).
---
<!-- COMMENTS:END -->
