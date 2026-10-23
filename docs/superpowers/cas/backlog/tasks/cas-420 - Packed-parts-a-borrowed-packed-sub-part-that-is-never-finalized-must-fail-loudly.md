---
id: CAS-420
title: >-
  Packed parts: a borrowed packed sub-part that is never finalized must fail
  loudly
status: To Do
assignee: []
created_date: '2026-10-23 01:50'
labels:
  - cas
dependencies: []
ordinal: 523000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From the packed-parts review (tmp/review3/REPORT.md item 7; deep #1, quality #2/#3, arch #5). Since d8f44574b18 beginTransaction/commitTransaction/undoTransaction on a borrowed DataPartStorageOnDiskPacked are no-ops; a caller that writes to the sub-part and skips precommit publishes a parent whose projection data.packed is missing or stale, with no exception (before: 'Cannot commit shared transaction'). is_precommitted is never reset on the shared path and a write after commit throws NOT_INITIALIZED; the committed transaction's overlay stays referenced for the part's lifetime. Every present caller finalizes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A modified, unfinalized borrowed packed writer is detected (chassert in the destructor path when no exception is in flight, or an unfinalized counter checked by the parent's precommit)
- [ ] #2 gtest: borrowed storage written and dropped without finalize fails; double commit and write-after-commit have pinned behavior
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
