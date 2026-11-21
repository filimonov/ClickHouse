---
id: CAS-183
title: >-
  Make `resolveRef` throw retry-later instead of reporting absent during the
  removal-latch window
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:ref-ledger'
  - 'area:read-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:2031-triage'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - R/Pool/CasRefLedger.cpp
priority: medium
type: bug
ordinal: 240000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Inside the removal-latch window `acquireReadableRefTableRuntime` returns `nullptr` (`R/Pool/CasRefLedger.cpp:621-623`, when `removal_admission_closed`), and `resolveRef` renders that as "no such ref" (`:293-299`).
`appendRefOps` throws the retry-later class for the same state. A reader sees "absent" for "temporarily not admitting".
Distinguish the latch from a namespace the catalog does not name, which should keep returning absent.

Provenance: BACKLOG/ref-protocol.md#lane-residuals-2031-cas-017 residual 1. Verified 2026-09-26 against b1c34d03479 and 0dbbd797792 (latch check at :627 there).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A resolve during the latch window throws the retry-later class; a resolve of an unknown namespace still returns absent (gtest for both)
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
First recorded: 2026-08-21 (7a9b839cd49, by 'lane-residuals-2031-cas-017')

2031-triage CAS-017: residual 1 (read path answers absent instead of retry-later while `dropNamespace` holds the admission latch).

2026-11-12: the late-request bundle (branch `fix/antalya-26.8/cas-late-request-audit-bundle`) makes this window longer. A `DROP` whose `Live -> Removing` write was sent and not answered now leaves the lane closed until a retried `DROP` completes or the disk remounts, not only for the length of the drop. The final review of that branch notes that reads on that server see the table as absent for that whole time (`acquireReadableRefTableRuntime` returns no runtime while the lane is closed). A retry-later answer from `resolveRef` matters more with that change.

2026-11-17: PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) keeps `removal_write_may_land` and adds `removing_observed` (c821aece830). A `beginRemoving` that lands after a remount leaves the row `Removing` under an open lane until a restart, a remount, a retried `DROP` or the GC leader's next round (accepted residual, d0e2d1a80bd). fc82bf0d80b latches a `Removing` row at the recovery confirmation.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: Pool/CasRefLedger.cpp:893-895 acquireReadableRefTableRuntime returns nullptr when removal_admission_closed; resolveRefOnRuntime (:359-373) turns a null runtime into nullopt (absent) with no distinction from an unnamed namespace. Changes: impact:availability. Pointers stale: CasRefLedger.cpp:621-623 and :293-299 moved to :893 and :372.
---
<!-- COMMENTS:END -->
