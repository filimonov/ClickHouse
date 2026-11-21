---
id: CAS-182
title: >-
  Re-resolve when a part-folder single-flight follower receives a view for
  another manifest
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:read-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:duplicate'
  - 'groom-20261121:certainty-high'
dependencies: []
references:
  - R/Parts/PartFolderAccess.cpp
  - R/Parts/PartFolderAccess.h
priority: medium
type: bug
ordinal: 239000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The single-flight key is `ns + ref` (`PartRefKey::cacheKey`, `R/Parts/PartFolderAccess.h:34`); a follower returns `future.get()` (`PartFolderAccess.cpp:258`) without checking the view's manifest id against the one it resolved.
Only the stale-tolerant `CachedForLoad` mode uses single flight and each view is single-manifest, so no mixed-manifest view exists.
Real effect: a follower straddling a repoint gets the neighbouring manifest's view and can surface a spurious `FILE_DOESNT_EXIST`.
Preferred fix: compare `manifestId()` after the wait and re-resolve on mismatch; keying by `ns+ref+manifest_id` is the alternative and loses sharing.

Provenance: BACKLOG/ref-protocol.md#part-folder-single-flight-manifest-keying (2031-triage CAS-019). Verified 2026-09-26 against b1c34d03479 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A follower whose resolved manifest differs from the leader's view re-resolves and gets its own view (gtest with a repoint between resolve and wait)
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
First recorded: 2026-08-21 (d8c5e8de79a, by 'part-folder-single-flight-manifest-keying')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: duplicate (high). Evidence: Parts/PartFolderAccess.cpp:252-263 follower returns future.get() keyed by cacheKey only, no manifestId check: the same defect and the same fix as CAS-323, which also asks for the read-your-writes audit and the gtest. Changes: none. Links: Duplicate of CAS-323 (newer, superset: adds the repoint-then-CachedForLoad audit). Defect itself still present..
---
<!-- COMMENTS:END -->
