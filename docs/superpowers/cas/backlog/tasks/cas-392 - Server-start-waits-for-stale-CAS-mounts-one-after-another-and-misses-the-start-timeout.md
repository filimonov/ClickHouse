---
id: CAS-392
title: >-
  Server start waits for stale CAS mounts one after another and misses the start
  timeout
status: To Do
assignee: []
created_date: '2026-10-15 05:26'
updated_date: '2026-10-15 05:42'
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
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-15 RCA (tmp/cas392/RCA.md): Context::getOrCreateDisk holds the server-wide storage_policies_mutex while it creates a disk(...) custom disk; a CAS disk runs its stale-mount wait (claimMountAwaitingExpiry, ~36.5 s at 30 s lease) inside it, so waits stack and every getDisk/getDisksMap/getStoragePolicy call stalls. The stress run drew async_load_databases=false, so 3 stale mounts (~110 s) blocked listen past the harness limit. Stale mounts came from a forced shutdown (safeExit skips disk teardown, no farewell). The wait itself is required (a lease cannot prove the predecessor dead). Failing tests: CASStartupStaleMounts.StaleMountsOfDifferentDisksAreWaitedOutConcurrently (red 10820 ms vs 6900) and DiskLookupDoesNotWaitForAStaleMountObservation (red 3603 ms vs 1150), local branch fix/antalya-26.6/cas-392-parallel-stale-mount-wait @ fe7435bc130. Recommended fix: create custom disks outside storage_policies_mutex with one shared creation per disk name (~40-60 lines in Context.cpp, fork patch to upstream code — needs owner approval). Config-defined disks are created serially in DiskSelector::initialize (code reading only).
<!-- SECTION:NOTES:END -->
