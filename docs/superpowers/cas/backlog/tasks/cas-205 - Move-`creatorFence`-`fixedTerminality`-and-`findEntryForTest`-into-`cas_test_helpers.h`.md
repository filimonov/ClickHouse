---
id: CAS-205
title: >-
  Move `creatorFence`, `fixedTerminality` and `findEntryForTest` into
  `cas_test_helpers.h`
status: To Do
assignee: []
created_date: '2026-07-31'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:testing'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:obsolete'
  - 'groom-20261121:certainty-medium'
milestone: m-4
dependencies: []
references:
  - src/Disks/tests/cas_test_helpers.h
  - src/Disks/tests/gtest_cas_ns_creation_lifecycle.cpp
  - src/Disks/tests/gtest_cas_ref_ckpt_join.cpp
priority: low
type: chore
ordinal: 262000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The three helpers are duplicated verbatim in `T/gtest_cas_ns_creation_lifecycle.cpp:33,41,46` and `T/gtest_cas_ref_ckpt_join.cpp:136,143,148`,
and both files already include `cas_test_helpers.h`. A fix to one copy silently misses the other.
The broader question of what belongs in the shared header was deferred as a convention question; this task settles only these three.

Provenance: BACKLOG/testing-and-ci.md#test-helper-third-copy; verified 2026-09-26 against 8b87aa15d21 and 8d62c314ec1.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each helper has one definition, in `cas_test_helpers.h`
- [ ] #2 `CAS*` gate green
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
First recorded: 2026-07-31 (a577bfffcc6, by 'test-helper-third-copy')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: obsolete (medium). Evidence: cas-pr1 src/Disks/tests: creatorFence, fixedTerminality and findEntryForTest are defined only in gtest_cas_ns_creation_lifecycle.cpp:33,41,46 and not referenced in gtest_cas_ref_ckpt_join.cpp or any header, so the duplication the task names is gone.. Changes: none. Pointers stale: gtest_cas_ref_ckpt_join.cpp:136,143,148 no longer hold these helpers.. Would settle it: Owner may still want the single copy moved into cas_test_helpers.h; that is not required by the stated problem..
---
<!-- COMMENTS:END -->
