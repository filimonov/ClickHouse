---
id: CAS-475
title: >-
  Stand test: a pool that mixes a build without the declared-lease rule (30 s
  lease) with a 40 s lease holder
status: To Do
assignee: []
created_date: '2026-11-13 03:02'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:lease'
  - 'origin:review'
  - 'area:testing'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: high
type: task
ordinal: 580000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Builds before v26.6.8.20003 (and antalya-26.8 builds before PR 2805) wait only their own lease and can fence a holder with a 120 s lease after about 40 s of stalled renewals. By reading the code the holder fails closed and remounts with a new epoch. Confirm on a stand: old build as GC leader, new build as holder, SIGSTOP the holder for 60 s, SIGCONT with an insert in flight; expect a token conflict on renewal, a local fence, a remount, and no write accepted after the fence. Record the result in the upgrade notes of docs/en/antalya/cas/configuration.md. Source: whole-branch review of wave 1 of PR https://github.com/Altinity/ClickHouse/pull/2831, tmp/prbundle/wave1/final/umbrella-aggregate.md (V-01, U-02).
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
2026-11-17: The default mount lease is 40 s since PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit feb86da1f4a (30 s -> 120 s -> 40 s). Earlier notes that name 120 s no longer describe the default. At 40 s a restart after a kill waits about 47 s and the write window in a store outage is 14 s. The mixed-lease case to test is 30 s with 40 s.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 docs/en/antalya/cas/configuration.md:325-345 documents the mixed-pool exposure and the pin to 30000 from code reading; no stand result (SIGSTOP scenario) in docs or tests/integration/test_cas_* (grep SIGSTOP empty). Changes: type unset -> task: a stand run. Pointers stale: the description's 120 s lease (the default is 40 s, per the title and configuration.md). Would settle it: a stand run record of the old-build-leader plus new-build-holder scenario.
---
<!-- COMMENTS:END -->
