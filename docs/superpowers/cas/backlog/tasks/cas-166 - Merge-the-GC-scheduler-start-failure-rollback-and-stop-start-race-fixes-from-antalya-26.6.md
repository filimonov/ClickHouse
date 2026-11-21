---
id: CAS-166
title: >-
  Merge the GC scheduler start-failure rollback and stop/start race fixes from
  antalya-26.6
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:27'
labels:
  - 'area:gc'
  - 'area:mounts'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:done'
  - 'groom-20261121:certainty-medium'
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-09-26 (391e4f95e92, by 'scheduler partial start')

`74c6f96c492` is the fix commit of PR #2326 (merged into antalya-26.6 as `919bd25e381`, 2026-09-23); neither is an ancestor of cas-gc-rebuild on 2026-09-26. The same PR closes 2031-triage CAS-050 on antalya-26.6.

2031-triage CAS-050: `CasGcScheduler::stop` joins `thread`/`hb_thread` outside `mutex` on cas-gc-rebuild (`CA/Gc/CasGcScheduler.cpp:101-112`), reachable via `SYSTEM CAS DROP POOL MEMBER` concurrent with GC STOP/shutdown; `74c6f96c492` (PR #2326) is an ancestor of antalya-26.6 only.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:27
---
Grooming 2026-11-21. Verdict: done (medium). Evidence: git log of src/Disks/tests/gtest_cas_gc_stop_start.cpp in cas-pr1 shows merge 71ce71a2504 of PR #2326 (the PR carrying 74c6f96c492); Gc/CasGcScheduler.cpp:102 and :141 take threads_mutex in start/stop. The two cited hashes themselves are not ancestors (merged under other hashes).. Changes: none. Would settle it: AC #2 start rollback of a partial start not read in code; AC #3 (the mounts-and-lifecycle items closed with the hash) not checked; CAS* gate result not recorded.
---
<!-- COMMENTS:END -->
