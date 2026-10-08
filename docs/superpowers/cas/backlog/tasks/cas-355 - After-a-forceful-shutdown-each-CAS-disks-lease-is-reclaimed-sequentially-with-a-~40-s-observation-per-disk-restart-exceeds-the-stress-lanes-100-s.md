---
id: CAS-355
title: >-
  After a forceful shutdown each CAS disk's lease is reclaimed sequentially with
  a ~40 s observation per disk; restart exceeds the stress lane's 100 s
status: To Do
assignee: []
created_date: '2026-10-08 20:51'
labels:
  - 'area:lifecycle'
  - 'area:ci'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:ci'
dependencies: []
priority: medium
type: bug
ordinal: 449000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On PR #2510 (stress amd_debug cas-s3), the stopped server logged `Will shutdown forcefully` with 6 connections left (17:14:41). The restart then waited 40.0 s for `stateless-ca-s3` (17:14:44 to 17:15:24), 40.0 s for `test_1_04292` inside a load job (17:15:26 to 17:16:06), and was still waiting for `test_h6x1p1d9_05054` (17:16:06.858) when the harness gave up at 17:16:34 ("Cannot start clickhouse-server", 10 tries x 10 s). Siblings took about 50 s (#2511, #2513) and about 91 s (#2509). Extends CAS-155, which covers only the startup `DiskSelector` loop; these were lazy mounts inside table loading.

Options:
(a) release every pool's lease on the forceful-shutdown path, so the next start sees a clean farewell;
(b) observe all disks in parallel, including lazily mounted ones (extends CAS-155);
(c) in the stress lane, drop the test-created CAS disks before the final restart, or give `start_server` a budget per CAS disk.

Not covered: whether `Will shutdown forcefully` skips lease release on every path (inferred from "stale-looking ... pid=6708" lines); the sibling wait counts (inferred from timing).

Evidence: tmp/lease-series/rca/ci-rca-2513-2510.md (Item 3); PR https://github.com/Altinity/ClickHouse/pull/2510.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A restart after a forceful shutdown with three or more CAS disks holding tables becomes ready within the stress lane's 100 s start budget
- [ ] #2 Lease reclaim cost after a forceful shutdown does not grow linearly with the number of CAS disks
- [ ] #3 The stress lane (cas-s3) no longer fails with Cannot start clickhouse-server on this path
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
