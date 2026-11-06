---
id: CAS-437
title: >-
  Serialize IDisk::checkAccess: custom disks created in parallel race on the
  shared probe file
status: To Do
assignee: []
created_date: '2026-11-06 15:26'
updated_date: '2026-11-06 16:22'
labels:
  - cas
  - ci
  - bug
dependencies: []
priority: high
ordinal: 540000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2805 CI (26.8 port of PR 2573, commit db921437e5e): after a restart in test_filesystem_cache the tables default.test (disk parallel_loading_test) and default.test_size_in_name (disk size_in_name_test), both cache disks over hdd_blob, fail to attach with 'Cannot open file .../disks/hdd_blob/clickhouse_access_check_<server uuid>' / 'Can't unlink file clickhouse_access_check_...' (FILE_DOESNT_EXIST) 'While checking access for disk ...'. Server log 12:15:44.907-45.104: threads 1649/1650/1651 (size_in_name_test, parallel_loading_test, truncated_size_in_name_test) write the same probe name on hdd_blob at once, all read the blob of the last writer, two of the three fail on unlink. PR 2573 moved custom-disk creation out of storage_policies_mutex (so creators of different names run concurrently); IDisk::checkAccess of every disk writes, reads and removes the same file name clickhouse_access_check_<server uuid>. antalya-26.6 carries 2573 since 2026-11-04 too; its CI has no test_cache_file_size_in_name, so the race is latent there. Evidence: tmp/lanes/ci/dl2805_5/access_check_race_12-15-44.log, tmp/lanes/REPORT.md. Fix: a process-wide mutex around the probe in IDisk::checkAccess (IDisk.cpp); config disks are created sequentially by DiskSelector, so it costs nothing on the upstream path. Unit test src/Disks/tests/gtest_disk_access_check.cpp (two ProbeDisk over one directory; max concurrent probes must be 1). Needs the same commit on antalya-26.6 (critical: tables fail to load at startup).
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
Fix committed on port/antalya-26.8/cas-from-26.6 as 086efc95363 (NOT pushed): static mutex around the probe in IDisk::checkAccess + src/Disks/tests/gtest_disk_access_check.cpp (DiskAccessCheck.ProbesOfDisksOverOneDirectoryDoNotOverlap). TDD: RED max_active=2 before the fix (build/test_access_check_red.log), GREEN after (test_access_check_green.log), 20/20 repeats (test_access_check_repeat.log), gate CAS*: 2904 passed, 0 failed (build/test_cas_gate_access_check.log). Release build only (disk). Open: push to PR 2805; same commit for antalya-26.6 (2573 is merged there since 11-04).

2026-11-06: 26.6 PR opened: https://github.com/Altinity/ClickHouse/pull/2812 (branch fix/antalya-26.6/disk-access-check-mutex @bb9bf738a0c, cherry-pick -x of 086efc95363; unit test 20/20, CAS* gate 2822 passed / 0 failed / 2 disabled). 26.8 fix is on PR 2805 @086efc95363. Open: watch CI of both.
<!-- SECTION:NOTES:END -->
