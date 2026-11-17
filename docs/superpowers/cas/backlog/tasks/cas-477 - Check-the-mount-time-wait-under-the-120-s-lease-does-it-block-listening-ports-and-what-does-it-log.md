---
id: CAS-477
title: >-
  Check the mount-time wait under the 120 s lease: does it block listening
  ports, and what does it log
status: To Do
assignee: []
created_date: '2026-11-13 03:02'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:lease'
  - 'origin:review'
dependencies: []
priority: medium
ordinal: 582000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After a hard kill a server waits out the old lease before it mounts a CAS disk (about 138 s with the 120 s default, 43 s before). The wait logs once at Information. Check on a stand whether the server listens during the wait, whether a supervisor start timeout can kill it, and whether progress logging is needed; update the upgrade notes. Source: whole-branch review of wave 1 of PR https://github.com/Altinity/ClickHouse/pull/2831, tmp/prbundle/wave1/final/umbrella-aggregate.md (V-07).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-13: answered in part by CI run 43379266824 of PR 2831: the wait is on the main thread before the server listens (port 9000 closed, status file held), logged twice at Information ('waiting ~131000 ms (token-stability observation)'); a 60 s start timeout plus a second start attempt hits the status-file lock. See the hard-restart task created today and tmp/prbundle/wave1/ci2/hard-restart.md.

2026-11-17: The default mount lease is 40 s since PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit feb86da1f4a (30 s -> 120 s -> 40 s). Earlier notes that name 120 s no longer describe the default. At 40 s a restart after a kill waits about 47 s and the write window in a store outage is 14 s.
<!-- SECTION:NOTES:END -->
