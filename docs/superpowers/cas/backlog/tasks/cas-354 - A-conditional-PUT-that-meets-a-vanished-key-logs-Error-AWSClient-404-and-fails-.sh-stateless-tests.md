---
id: CAS-354
title: >-
  A conditional PUT that meets a vanished key logs <Error> AWSClient 404 and
  fails .sh stateless tests
status: To Do
assignee: []
created_date: '2026-10-08 20:51'
labels:
  - 'area:backend'
  - 'area:tests'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:ci'
dependencies: []
priority: medium
type: bug
ordinal: 448000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`ObjectStorageBackend::nativeConditionalPut` (`CasObjectStorageBackend.cpp`) runs without an `Expect404ResponseScope`, so `PocoHTTPClient.cpp` logs a 404 answer to an `If-Match` `PUT` at Error. The scope is set only around the reads. Related: CAS-168.

Seen on PR #2513 asan cas-s3 1/2, `00652_mutations_alter_update` (17:33:12.852): key `cas_s3/blobs/ch128/29/29a28b88145cc921633824377ebd7d48.meta`, 412 at 17:33:12.801 then 404 at 17:33:12.852. The CAS engine treats the 404 as a store answer and resolves it; the INSERT succeeded. The line reaches the client through `send_logs_level=warning` and fails the `.sh` test. 1 occurrence in 10 shard runs (#2509-#2513).

Fix: wrap the conditional write in `Expect404ResponseScope`, as the reads already are. Also check `WriteBufferFromS3`'s own NO_SUCH_KEY retry ("Single part upload failed with NO_SUCH_KEY error ... will retry"), which reissues a conditional `PUT` behind the engine's back. Not covered: who deleted the `.meta` between the 412 and the reissue.

Evidence: tmp/lease-series/rca/ci-rca-2513-2510.md (Item 2); PR https://github.com/Altinity/ClickHouse/pull/2513.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The 404 of a conditional PUT whose precondition object vanished is logged at Debug/Trace
- [ ] #2 00652_mutations_alter_update under cas-s3 does not fail on that line
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
