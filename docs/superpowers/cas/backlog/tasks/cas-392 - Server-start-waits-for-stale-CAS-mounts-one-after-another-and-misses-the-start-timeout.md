---
id: CAS-392
title: >-
  Server start waits for stale CAS mounts one after another and misses the start
  timeout
status: Done
assignee: []
created_date: '2026-10-15 05:26'
updated_date: '2026-11-04 22:39'
labels:
  - 'area:mounts'
dependencies: []
priority: high
ordinal: 490000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Stress test (amd_debug) on https://github.com/Altinity/ClickHouse/pull/2516 (head b7e424247ab): the server was shut down forcefully and 3 CAS mounts stayed unclean. On restart it waited for each stale mount in turn, about 36.5 s each (30 s lease + 5% + poll step, "token-stability observation"), about 110 s in total. The stress harness gives up on start after about 100 s and reports "Cannot start clickhouse-server". The wait grows linearly with the number of stale mounts. #2516 does not lengthen it (declared and own lease are both 30 s). Triage: tmp/ci-status/report-2513-2516.md. Root cause investigation in progress.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [x] #2 ASan lane green for touched suites
- [x] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [x] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-15 RCA (tmp/cas392/RCA.md): Context::getOrCreateDisk holds the server-wide storage_policies_mutex while it creates a disk(...) custom disk; a CAS disk runs its stale-mount wait (claimMountAwaitingExpiry, ~36.5 s at 30 s lease) inside it, so waits stack and every getDisk/getDisksMap/getStoragePolicy call stalls. The stress run drew async_load_databases=false, so 3 stale mounts (~110 s) blocked listen past the harness limit. Stale mounts came from a forced shutdown (safeExit skips disk teardown, no farewell). The wait itself is required (a lease cannot prove the predecessor dead). Failing tests: CASStartupStaleMounts.StaleMountsOfDifferentDisksAreWaitedOutConcurrently (red 10820 ms vs 6900) and DiskLookupDoesNotWaitForAStaleMountObservation (red 3603 ms vs 1150), local branch fix/antalya-26.6/cas-392-parallel-stale-mount-wait @ fe7435bc130. Recommended fix: create custom disks outside storage_policies_mutex with one shared creation per disk name (~40-60 lines in Context.cpp, fork patch to upstream code — needs owner approval). Config-defined disks are created serially in DiskSelector::initialize (code reading only).

2026-10-15: PR https://github.com/Altinity/ClickHouse/pull/2573 (fix/antalya-26.6/cas-392-parallel-stale-mount-wait @ c905ab39e7a): custom disks created outside storage_policies_mutex, single-flight per name, reload skips in-flight names. Config-defined disks still serial (DiskSelector::initialize); one stale mount still costs ~36.5 s.

2026-11-04: PR https://github.com/Altinity/ClickHouse/pull/2573 merged as 52c95e21002 (antalya-26.6). CI on head b558a36234a: no red caused by the change; the TSan relink and GroupingAggregatedTransform reds are base-branch defects (see the PR triage notes in the 2573 thread).
<!-- SECTION:NOTES:END -->
