---
id: CAS-166
title: >-
  Merge the GC scheduler start-failure rollback and stop/start race fixes from
  antalya-26.6
status: To Do
assignee: []
created_date: '2026-09-26 07:41'
labels:
  - 'area:gc'
  - 'area:mounts'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.cpp
  - src/Disks/tests/gtest_cas_gc_stop_start.cpp
priority: high
type: bug
ordinal: 212000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Commit `74c6f96c492` on `altinity/antalya-26.6` fixes `CasGcScheduler::start` (partial start is rolled back on failure) and the `stop`/`start` race, with tests in `src/Disks/tests/gtest_cas_gc_stop_start.cpp`. `cas-gc-rebuild` still carries the racy `start` and `stop` (mounts-and-lifecycle.md items "scheduler partial start" and "stop/join race"), and no other task tracks bringing that commit over. Cherry-pick or merge it, keep the tests, and confirm the CAS* gate on both sanitizer lanes.

Provenance: BACKLOG/mounts-and-lifecycle.md scheduler partial start + stop/join race; fix exists only on antalya-26.6 (review of u10, 2026-09-26).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `74c6f96c492` (or an equivalent) is on cas-gc-rebuild and `gtest_cas_gc_stop_start.cpp` passes in the CAS* gate
- [ ] #2 `CasGcScheduler::start` rolls back a partial start and `stop` cannot race a concurrent `start`
- [ ] #3 The two mounts-and-lifecycle items are closed with the commit hash
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
