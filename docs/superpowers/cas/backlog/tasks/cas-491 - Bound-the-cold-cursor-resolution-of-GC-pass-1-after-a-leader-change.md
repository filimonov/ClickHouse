---
id: CAS-491
title: Bound the cold cursor resolution of GC pass 1 after a leader change
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:plausible'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies:
  - CAS-487
priority: medium
type: enhancement
ordinal: 596000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
GC pass 1 resolves each cold cursor with sequential `GET`s and is not cut by the intake time slice `kRefIntakeBudgetMs`. Every GC leader start is cold, so a pool with many namespaces pays this on each leader change. Needs a design choice (prefetch, or a slice for pass 1) and a measurement on many namespaces. Follow-up of CAS-487; see also CAS-29.10.
<!-- SECTION:DESCRIPTION:END -->

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
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 Gc/CasGc.cpp:2537 comment says pass 1 gives every namespace its start and one read window and is never cut; kRefIntakeBudgetMs (CasGc.h:441, used at CasGc.cpp:2488) bounds pass 2 only. Changes: area set to gc; size unset -> medium (M): design choice plus a many-namespace measurement; risk set to medium; confidence set to plausible; impact:availability; type unset -> enhancement: bound an existing phase; priority unset -> medium: no priority set; cost per leader change on large pools. Size: M. Links: See CAS-29.10. Would settle it: Measurement of cold pass 1 time on a pool with many namespaces.
---
<!-- COMMENTS:END -->
