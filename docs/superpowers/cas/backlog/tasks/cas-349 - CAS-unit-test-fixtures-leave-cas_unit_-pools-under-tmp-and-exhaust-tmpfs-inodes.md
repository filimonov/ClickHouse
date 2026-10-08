---
id: CAS-349
title: >-
  CAS unit-test fixtures leave cas_unit_* pools under /tmp and exhaust tmpfs
  inodes
status: To Do
assignee: []
created_date: '2026-10-08 15:53'
labels:
  - 'area:tests'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:incident'
dependencies: []
priority: medium
type: bug
ordinal: 443000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`makeLocalObjectStorageRoot` (`src/Disks/tests/cas_test_helpers.h`) and the three sibling helpers in `gtest_cas_backend_generation.cpp` and `gtest_cas_upstream_slice.cpp` create `temp_directory_path()/cas_unit_<pid>_<n>` and return a bare `LocalObjectStorage`; nothing removes the directory afterwards (`remove_all` runs only before creation). Every `CAS*` gate leaves ~150 directories; on 2026-10-08 the accumulated debris (12 957 top-level entries) exhausted the 1 M inodes of the tmpfs `/tmp` and every tool on the host failed with ENOSPC. Fix: the helpers return the storage together with a guard that `remove_all`s the root in its destructor (a removal error is logged, never thrown — teardown must not fail a green test). Workaround until then: run the gates with `TMPDIR=<build>/tmp`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 No cas_unit_* directory remains under TMPDIR after a full CAS* gate run (release and ASan)
- [ ] #2 A removal failure in the guard is logged and does not fail the test
- [ ] #3 The four helper sites all use the guard
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
