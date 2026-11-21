---
id: CAS-345
title: >-
  Say at the graduation gate and at its test how old the read-ahead marker value
  may be (issue #2486)
status: To Do
assignee: []
created_date: '2026-10-07 20:29'
updated_date: '2026-11-21 08:20'
labels:
  - cas
  - gc
  - docs
  - 'area:gc'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - groomed-20261121
  - 'groom-20261121:obsolete'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies:
  - CAS-30
priority: low
type: docs
ordinal: 439000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Issue https://github.com/Altinity/ClickHouse/issues/2486 read `confirm_condemned_marker` (CasGc.cpp, CAS-30 branch) as a stale decision: the gate accepts the marker value the graduation read-ahead fetched, not a GET issued at the gate. The behaviour is intended and covered: the GET is issued after the round cut when a read-ahead slot is free (window = gc_io_concurrency + 1), so the value is at most a window of graduating entries old, inside the same fold; a `Clean` landing in that gap belongs to the 10.1/10.2 residuals of the CAS-30 spec (extdrop, late `Clean If-Match`), which a read at the gate does not close either, and which CAS-334 closes. Model: `MarkerPrefetch` flag of `CaGcCondemnMarkerGate` (363b4193634), all green rows stay green, the only violation is the 10.1 residual.

The comment at `confirm_condemned_marker` and the test `GraduationReadAheadDecidesOnTheFetchedValue` (gtest_cas_gc_read_ahead.cpp) say "read after the cut" but not the staleness bound or why it is covered. Prose only, no behaviour change. Promised in the issue reply.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Comment at `confirm_condemned_marker` states: value from the read-ahead, at most a window of entries old, after the cut; a `Clean` in that gap is spec 10.1/10.2, closed by CAS-334
- [ ] #2 Comment at `GraduationReadAheadDecidesOnTheFetchedValue` states what it pins and why that is the intended behaviour, with the same pointer
- [ ] #3 No code change; `CAS*` gate unchanged
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
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: obsolete (high). Evidence: cas-pr1 src/Disks: grep finds no confirm_condemned_marker and no GraduationReadAheadDecidesOnTheFetchedValue; tests/gtest_cas_gc_read_ahead.cpp now holds only CASGCReadAhead.* tests (Hit/Miss/Duplicate/...); Gc/CasGc.cpp keeps only the CASGCRetiredGraduated counter (lines 52, 121). The commented code and test do not exist. Changes: area set to gc; size unset -> trivial (XS): n/a, subject gone; risk set to low; confidence set to solid; type unset -> docs: comment-only change. Size: XS. Links: CAS-334 (the fix this comment points to) is also obsolete on this branch.
---
<!-- COMMENTS:END -->
