---
id: CAS-207
title: >-
  Give CAS gtests a checked accessor so a disengaged optional fails the test
  instead of aborting the binary
status: To Do
assignee: []
created_date: '2026-08-04'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:testing'
  - 'area:ci'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - quick-win
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - src/Disks/tests/cas_test_helpers.h
  - src/Disks/tests/gtest_cas_orphan_nomination.cpp
  - src/Disks/tests/gtest_cas_gc_frontier_gate.cpp
priority: medium
type: task
ordinal: 264000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A gtest that dereferences an empty `std::optional` aborts the process; every later test in the binary never runs and the gate reports a
smaller total that still reads green. This hid suites three times in one night.
Measured 2026-09-26: `EXPECT_TRUE(x.has_value())` appears 16 times against 489 `ASSERT_TRUE`; confirmed unguarded sites remain at
`T/gtest_cas_orphan_nomination.cpp:184`, `:188` and, for iterators after `EXPECT_NE(it, end)`, `T/gtest_cas_gc_frontier_gate.cpp:1350`, `:1403`.
In a `void` body `ASSERT_*` is enough; a non-`void` helper must `EXPECT` then bail (`sealedCursorOf`/`holdOf` in `gtest_cas_gc_hold_grammar.cpp`).
A one-off sweep does not close the class; the next test reintroduces it.

Provenance: BACKLOG/testing-and-ci.md#cas-tests-unchecked-optional-deref; counts re-measured 2026-09-26 against 8b87aa15d21.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A checked accessor in `cas_test_helpers.h` records a test failure and stops the test on an empty value
- [ ] #2 The confirmed sites use it or an `ASSERT_*`, and each remaining `EXPECT_TRUE(x.has_value())` is followed by a guard
- [ ] #3 A deliberately emptied optional in one test leaves the rest of the `CAS*` run executing and counted
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
First recorded: 2026-08-04 (55a6903535b, by 'cas-tests-unchecked-optional-deref')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 src/Disks/tests/cas_test_helpers.h has no checked optional accessor (only an ASSERT_TRUE(got.has_value()) inside one helper at :449); gtest_cas_orphan_nomination.cpp:174 and :178 still use EXPECT_TRUE(x.has_value()) before dereferencing.. Changes: label quick-win. Pointers stale: orphan_nomination sites :184/:188 are now :174/:178..
---
<!-- COMMENTS:END -->
