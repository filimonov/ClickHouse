---
id: CAS-222
title: >-
  Bring the `MemoryWorker` sanitizer-allocated fix (#2349) to cas-gc-rebuild and
  confirm the ASan CAS lane has no memory-limit failures
status: To Do
assignee: []
created_date: '2026-09-26 07:57'
labels:
  - 'area:ci'
  - 'area:upstream'
  - 'complexity:trivial'
  - 'risk:low'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'origin:issue'
milestone: m-8
dependencies: []
references:
  - src/Common/MemoryWorker.cpp
  - 'https://github.com/Altinity/ClickHouse/pull/2349'
  - 'https://github.com/Altinity/ClickHouse/issues/2299'
  - 'https://github.com/Altinity/ClickHouse/pull/2300'
priority: medium
type: task
ordinal: 280000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On the ASan CAS stateless lane `MemoryTracking` jumped from 1.8 MB to 15.4 GiB in one second and stayed there, failing ordinary tests with
`(total) memory limit exceeded` (PR #2300 run 8, 22 failures on two shards). Cause: the non-jemalloc `MemoryWorker` branch replaces a negative tracker
with resident memory, which is the ASan-inflated RSS. Not CAS-specific.
Fixed on antalya-26.6 by `0c9c89743cc` (PR #2349, closes #2299): the correction uses `__sanitizer_get_current_allocated_bytes`.
cas-gc-rebuild still has `MemoryTracker::updateAllocated(resident, ...)` (`src/Common/MemoryWorker.cpp:913-916`) and is 137 commits behind antalya.

Provenance: BACKLOG/testing-and-ci.md#asan-memory-tracker-snap, candidate (a); verified 2026-09-26 against 8b87aa15d21 and 8d62c314ec1.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 cas-gc-rebuild's `MemoryWorker` uses the sanitizer allocated-bytes value in the negative-tracker correction
- [ ] #2 One ASan CAS stateless run shows no one-second `MemoryTracking` jump and no memory-limit failures on ordinary tests
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
