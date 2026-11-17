---
id: CAS-495
title: Remove plan and review tags from CAS code comments
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-17 20:02'
labels: []
dependencies:
  - CAS-487
ordinal: 600000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The files touched by CAS-487 still carry 35 lines with plan or review tags and 117 lines with `INV-*` or `spec §` tags in product code. Remove the tags and keep the reason each one pointed at, so the comment states the constraint in plain words. Follow-up of CAS-487.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 No plan, review, `INV-*` or `spec §` tag remains in the product files CAS-487 touched
- [ ] #2 Each removed tag left its reason in the comment
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
2026-11-15: done for product files the CAS-487 branch touched (commit 14 of 14): plan and review citations removed from comments, 93 matches to 0; INV-* names and spec section references stay; test files not swept. Close this task when CAS-487 merges, or keep it only for the test files.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Plan and review tags removed from the touched product files in `97123265e03`. `INV-*` names and spec section references stay; test files are not swept.
<!-- SECTION:NOTES:END -->
