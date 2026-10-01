---
id: CAS-341
title: >-
  CAS read retry loop retries MEMORY_LIMIT_EXCEEDED thrown inside
  ReadBufferFromS3 until the policy deadline and rethrows it as CANNOT_COMMIT
status: To Do
assignee: []
created_date: '2026-10-01 17:57'
labels:
  - gc
  - bug
  - ci
dependencies: []
priority: high
ordinal: 434000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Observed 2026-10-01 on PR #2463, job "Stateless tests (arm_binary, cas s3 storage, parallel)", test 04023_rollup_totals_memory_limit (max_memory_usage 1 MB, under randomized MergeTree settings prewarm_* / use_primary_key_cache). Expected MEMORY_LIMIT_EXCEEDED (241); got 210 "CAS write could not be committed (read of ...manifests/.../000001.zst: gave up at the policy deadline after 38 attempt(s))". The manifest GET throws 241 inside ReadBufferFromS3; the CAS read retry treats it as transient, retries 38 times to the deadline, then wraps it as 210. A caller-side resource error (memory limit, query cancelled, KILL) is not transient: it must propagate at once with its own code, as the transaction-metadata retry does for LOGICAL_ERROR and NOT_IMPLEMENTED. Evidence: tmp/ci-2463/rca.md section 7, tmp/ci-2463/cas_s3.log query b42da83c. Not caused by #2463 (manifest read path, no GC involvement); amd_binary cas s3 passed on #2463/#2452/#2455, the arm lane had no baseline.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The CAS read/write retry loops rethrow MEMORY_LIMIT_EXCEEDED, QUERY_WAS_CANCELLED and the other caller-side errors immediately with the original code
- [ ] #2 A gtest injects MEMORY_LIMIT_EXCEEDED from the backend read and asserts one attempt and code 241
- [ ] #3 04023_rollup_totals_memory_limit passes on the cas s3 storage lane under the randomized settings that failed
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
