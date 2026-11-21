---
id: CAS-401
title: Pass object_storage_attempt_number from engine attempts to the HTTP client
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:observability'
  - 'area:backend'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: low
ordinal: 499000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`object_storage_attempt_number` is not passed, so every engine attempt in `ensureBlobPresent` looks like attempt 1 to the HTTP client.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Engine attempt n reaches the HTTP client as attempt n
<!-- AC:END -->

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
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1: Backend/CasObjectStorageBackend.cpp:623 and :841 pass object_storage_attempt_number for backend requests, but Pool/CasPartWriteTxn.cpp (home of ensureBlobPresent) has no attempt_number / attempt_no reference. Changes: impact:operability. Would settle it: Not traced whether the blob PUT in ensureBlobPresent goes through a backend call that already receives the engine attempt (the :841 path would then cover it)..
---
<!-- COMMENTS:END -->
