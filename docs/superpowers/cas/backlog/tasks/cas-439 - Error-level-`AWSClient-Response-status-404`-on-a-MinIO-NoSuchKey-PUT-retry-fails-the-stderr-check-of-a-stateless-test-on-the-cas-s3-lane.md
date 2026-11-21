---
id: CAS-439
title: >-
  Error-level `AWSClient: Response status: 404` on a MinIO NoSuchKey PUT retry
  fails the stderr check of a stateless test on the cas s3 lane
status: To Do
assignee: []
created_date: '2026-11-06 22:01'
updated_date: '2026-11-21 08:25'
labels:
  - cas
  - ci
  - 'area:write-path'
  - 'theme:ci-stabilization'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
dependencies: []
priority: medium
type: bug
ordinal: 542000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
03100_lwu_50_subcolumn_skip_index failed once on PR 2577 @74562b9f124, job `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)`, reason `having stderror: <Error> AWSClient: Response status: 404, Not Found` (query dff55266-dfe9-46d3-a4bc-eecb3ce5d36b, INSERT of 10000 rows, 03:39:14). Server log, thread 3265, key `cas_s3/blobs/ch128/11/115d957b24ae5d2395d1d2ae954c033f.meta` (82 bytes): PUT with If-None-Match -> 412 (dedup hit, logged at Debug); the next PUT of the same key -> 404 NoSuchKey, logged by PocoHTTPClient at Error; then `WriteBufferFromS3: Single part upload failed with NO_SUCH_KEY error` (Information), a GET -> NoSuchKey (Debug), `Retrying S3 request ... clickhouse attempt: 2, kind: Write`; the insert succeeded, 23 reruns passed. No GC round and no delete of that key in the log window (the only cas_gc_log entry at 03:39:10 is for the per-test disk test_l1pu05rw_04279_cas_gc). The 412 count in that shard's log is 961, the Error-level 404 count is 1. Upstream already treats a NoSuchKey on PUT as a MinIO quirk and retries (`src/IO/WriteBufferFromS3.cpp`, 'For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests'); the Error line comes from the HTTP client, not from the retry loop, so the retry that succeeds still fails the test. Log: https://altinity-build-artifacts.s3.amazonaws.com/PRs/2577/74562b9f1244f2b7b585c255106316f4ddabb2d6/stateless_tests_amd_asan_ubsan_cas_s3_storage_parallel_2_2/clickhouse-server.log.zst
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decide and document why MinIO answers 404 to the PUT that follows a 412 on the same key (concurrent identical blobs from one insert are the suspected trigger)
- [ ] #2 A PUT answered with NoSuchKey that the write path retries is not logged at Error; a test pins the level
- [ ] #3 The cas s3 stateless lanes stop failing on this line
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 src/IO/S3/PocoHTTPClient.cpp:741 still logs an unexpected 404 at LOG_ERROR irrespective of request kind; WriteBufferFromS3.cpp:832-835 still retries NO_SUCH_KEY on PUT; the task records a single occurrence (23 reruns passed). Changes: none. Links: related to the S3 retry shared code in src/IO/S3 (touches upstream code; no upstream query: no safe search term beyond the file names). Would settle it: whether the failure recurred on later CI runs of the cas s3 lane (one occurrence on PR 2577 only); and whether upstream ClickHouse changed that HTTP-client log level, which would make a CAS-side chan….
---
<!-- COMMENTS:END -->
