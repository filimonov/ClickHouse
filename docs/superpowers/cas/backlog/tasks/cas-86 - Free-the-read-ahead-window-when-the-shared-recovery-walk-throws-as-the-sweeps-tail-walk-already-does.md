---
id: CAS-86
title: >-
  Free the read-ahead window when the shared recovery walk throws, as the
  sweep's tail walk already does
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:done'
  - 'groom-20261121:certainty-medium'
milestone: m-1
dependencies:
  - CAS-358
references:
  - CA/Pool/CasRefProtocol.cpp
  - CA/Gc/CasOrphanManifestSweep.cpp
priority: low
type: bug
ordinal: 121000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`recoverRefTableDetailedFromAuthority` (`CA/Pool/CasRefProtocol.cpp:1042`) hints ref-log ids and discards them only at a seal
(`:1075`, `:1101`). A `CORRUPTED_DATA` or decode throw leaves up to one window pinned, and `planManifestCursorPage` catches
per-namespace throws and keeps the same reader for the rest of the page, so later reads degrade to serial.
Throughput only. The `OutstandingHintGuard` the sweep's own tail walk uses (`CA/Gc/CasOrphanManifestSweep.cpp:305-326`) closes it.

Provenance: BACKLOG/gc.md#gc-condemn-head-read-ahead-pinned-window (sibling paragraph). Verified 2026-09-26 against 59494ebf366 and 0dbbd797792 (no guard on either).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest: a recovery walk that throws mid-window leaves `pending() == 0` on the shared reader
- [ ] #2 A following namespace on the same page reads through the read-ahead with no extra misses
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
First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

Identifier trace: the earliest docs mention of `CORRUPTED_DATA` is 2026-06-03 (87be5558142); the finding itself has no record before this migration, so the creation date stays 2026-09-26.

2026-10-13: absorbed by CAS-358 §5: the sweep's OutstandingHintGuard moves into recoverRefTableDetailedFromAuthority because the fold's seed walk runs on the shared read-ahead reader. Keep open until that lands; AC unchanged.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: done (medium). Evidence: Pool/CasRefProtocol.cpp:914-921 discard_hints + SCOPE_EXIT_MEMORY_SAFE discards outstanding ref-log hints on any exit of the recovery walk (the CAS-358 absorption); OutstandingHintGuard no longer exists anywhere in ContentAddressed/; gtest_cas_ref_protocol.cpp:386 asserts reader.pending() == 0. Changes: none. Pointers stale: CasRefProtocol.cpp:1042/1075/1101 and CasOrphanManifestSweep.cpp:305-326 no longer match. Would settle it: not confirmed that the :386 test throws mid-window (AC #1) nor that a following namespace on the same page reads without extra misses (AC #2); not confirmed that the guarded function is the one recov….
---
<!-- COMMENTS:END -->
