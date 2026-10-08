---
id: CAS-352
title: Confirm a slot's absence with a LIST before a terminal stop
status: To Do
assignee: []
created_date: '2026-10-08 16:49'
labels:
  - 'area:mounts'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-8
dependencies: []
priority: medium
type: enhancement
ordinal: 446000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 6 of the lease series (absence only on a named `NoSuchKey`) was withdrawn by the owner on 2026-10-08 as too format-sensitive: an unnamed 404 or `NotFound` store would never mount, and upstream accepts both. Replacement for series PR 12: `ProvenAbsent` = the GET answered 404 (named or not) AND a LIST of the key's prefix does not contain the key; a failed or 404 LIST is `NotObserved` and does not stop the server. Applies to `slot_absent`, `pool_identity_lost` and the reacquire anchor/counter checks. Tests use the wire-level HTTP harness from the withdrawn branch `cas-lease-pr06-absence-per-verb` (proxy answering 404 to everything gives `NotObserved`; GET 404 plus LIST without the key stops).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A proxy 404 on GET and LIST never stops the server
- [ ] #2 GET 404 with the key absent from the LIST stops the server
- [ ] #3 No bucket requirement on the 404 body
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
