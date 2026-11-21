---
id: CAS-483
title: >-
  Count the mount-lease wait from the slot's age on the store's clock
  (`Last-Modified` against the response `Date`)
status: To Do
assignee: []
created_date: '2026-11-13 16:28'
updated_date: '2026-11-21 08:26'
labels:
  - 'area:lease'
  - 'origin:owner'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: design
ordinal: 588000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Owner idea (2026-11-13). Today a starting server waits the full observation threshold from its own first sighting of the slot token, whatever the slot's age: a lease that ended hours ago still costs the full wait (about 135 s at the 120 s lease). Both timestamps of one GET of the mount slot come from the store: the object's Last-Modified and the response's Date; their difference is the age of the last renewal on the store's clock. The holder counts its lease from the start of that renewal, before the write lands, so the store-side age is never larger than what the holder itself has counted: waiting 'threshold minus age' is conservative. No clock is compared between two processes. Covers: a server down for longer than the lease (wait about 0), supervisor restart loops (only the remainder), on any file system including overlayfs. Does not cover: a restart seconds after a kill (the lease is still valid by every clock); that is CAS-482. To check: Last-Modified has 1 s resolution (margin); multi-node self-hosted stores (MinIO, RustFS) may stamp Date and Last-Modified on nodes with skewed clocks (margin or a setting); clock rate difference between store and holder (the existing 1.05 factor); GCS through the S3 API. Small: one read that already happens plus arithmetic; needs its own short spec and the mount observation model.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-17: The default mount lease is 40 s since PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit feb86da1f4a (30 s -> 120 s -> 40 s). Earlier notes that name 120 s no longer describe the default. At 40 s a restart after a kill waits about 47 s and the write window in a store outage is 14 s.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:26
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 Pool/*.cpp and Backend/CasProbe.cpp: no use of Last-Modified or the response Date in the mount observation (grep empty), so the wait is still counted from the first sighting of the token. Changes: type unset -> design: the task says it needs its own short spec and the mount observation model first. Links: related CAS-482 (restart seconds after a kill, not covered here). Pointers stale: description assumes a 120 s lease (default now 40 s, feb86da1f4a).
---
<!-- COMMENTS:END -->
