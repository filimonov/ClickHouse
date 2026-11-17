---
id: CAS-400
title: >-
  Verify that staged-copy privilege is not shared by hardlink copies with the
  same staging key
status: Done
assignee: []
created_date: '2026-10-15 11:41'
updated_date: '2026-11-17 20:01'
labels:
  - 'area:write-path'
  - 'area:backend'
  - 'needs:repro'
  - 'complexity:medium'
  - 'risk:medium'
  - 'origin:review'
dependencies: []
priority: medium
ordinal: 498000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Staged-copy privilege is per `BlobSource`. Hardlink copies push a duplicate `PendingBlob` with the same `staging_key` into another part, so a second verbatim copy of the same staging may obtain the same token. Pre-existing, outside the #2555 diff. Verify with a test, then fix or close.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Test shows a second copy of the same staging cannot reuse the privilege, or the defect is fixed
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
2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Fixed in PR 2831 by `a966a374b17`: the once-only flag moved from `BlobSource` to `PendingBlob`, so hardlink copies share it and only the first publication copies the staging object verbatim.
<!-- SECTION:NOTES:END -->
