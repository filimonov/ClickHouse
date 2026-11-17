---
id: CAS-488
title: >-
  Drop the catalog `GET` from the positive ref append (Part B of the one-request
  append)
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-17 20:02'
labels: []
dependencies:
  - CAS-487
ordinal: 593000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Part B of CAS-487 (spec `docs/superpowers/specs/2026-11-14-cas-one-request-ref-append-design.md`, section 7). After CAS-487 a positive ref append costs 2 requests plus a catalog `GET`. Part B writes the terminal removal record first, so the append no longer needs the catalog read. It also subsumes CAS-176.1, CAS-454 and CAS-367, and rewrites the code of CAS-367 and CAS-455.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A positive append issues no catalog `GET`
- [ ] #2 Refusal after the namespace own `DROP` and after a remount is kept (pinned by `.PositiveAppendReadsNoCatalog`)
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
2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Delivered in PR 2831 by `d0e2d1a80bd` and `c821aece830`, except the named test: the own-DROP refusal is pinned by another test name; add a catalog-count assertion to a remount refusal test, then close.
<!-- SECTION:NOTES:END -->
