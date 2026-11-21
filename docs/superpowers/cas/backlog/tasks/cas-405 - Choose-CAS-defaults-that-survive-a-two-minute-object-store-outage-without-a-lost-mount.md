---
id: CAS-405
title: >-
  Choose CAS defaults that survive a two-minute object-store outage without a
  lost mount
status: In Progress
assignee: []
created_date: '2026-10-18 13:52'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:mounts'
  - 'risk:medium'
  - 'confidence:solid'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: high
type: task
ordinal: 503000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Goal: losing connectivity to the object store for about 2 minutes passes without a lost mount, without a remount and without client-visible write errors beyond a pause. Today: cas_mount_lease_ttl_ms 30 s, writes give up at the 90 s policy deadline, an attempt is bounded by the disk request_timeout_ms (30 s); RustFS read stalls of 90-100 s already fail CI (#2343). #2516 (observers wait for the declared lease, holder cap 10 min) makes a longer lease safe; #2517 turns an expired lease into a pause. Decide lease TTL and renew period, the write policy deadline, attempt timeouts; cost: slower detection of a dead member, longer GC floor hold. Context: docs/superpowers/cas/2026-10-18-open-items.md#defaults-two-minute-outage.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 On the fault-proxy stand with the store unreachable for 120 s under inserts, merges and selects: no CASMountLeaseLost, no remount, no client error other than a documented pause; new defaults documented in configuration.md
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
Experiments done 2026-10-18: tmp/cas405/REPORT.md (20 runs, base vs #2517). #2517 removes lease loss and remount for full outages of 60-180 s; oracle and fsck clean in every run. Remaining work split into CAS-405.1 (fetch/cleanup busy loop), .2 (pause = wait, not error), .3 (defaults 120/40 s), .4 (fast reclaim after crash), .5 (_ckpt give-up text). Stand fd limit fixed in 0343193ee3c.

2026-11-11 defaults review: 26.8 still fails fast (first INSERT error 11-19 s into an outage at 30/10). Write-survival rule measured: writes continue for lease - 16 s reserve - renew period (guaranteed) to lease - 16 s (best case). 120 s lease with a 10 s renew period guarantees 94 s; validated on 12 fault runs: no client error up to 60 s outages, 6 s PUT delay and 10 % random faults; 2-4 errors on 90 s faults, all from the 90 s request window (new subtask CAS-405.6). Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (worktree ~/workspace/ClickHouse/cas-297-defaults); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-17: The default mount lease is 40 s since PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit feb86da1f4a (30 s -> 120 s -> 40 s). Earlier notes that name 120 s no longer describe the default. At 40 s a restart after a kill waits about 47 s and the write window in a store outage is 14 s. The parent goal (a 120 s outage without a client error) is not met by this default.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: ContentAddressedSettings.cpp:80 default lease 40000 (note 2026-11-17: 120 s outage goal not met by it); request_window_ms already a setting at :93 with default 120000. Changes: risk set to medium; confidence set to solid; type unset -> task: type missing; parent of measured default choices. Links: Children CAS-405.1 to .6; status In Progress, last note 2026-11-17. Would settle it: a 120 s fault-proxy run on current defaults, and the state of children .2 .4 .6.
---
<!-- COMMENTS:END -->
