---
id: CAS-396
title: Measure CompleteMultipartUpload over 30 s for large blobs on RustFS and MinIO
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
labels:
  - 'area:write-path'
  - 'area:backend'
  - 'needs:measurement'
  - 'complexity:small'
  - 'origin:review'
dependencies: []
priority: medium
ordinal: 494000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Blob `CompleteMultipartUpload` now runs on the single-attempt creation client. Before, a slow Complete was rescued by retry plus NoSuchUpload then HEAD; now it can cost up to 8 full re-uploads and end in a retryable error. Tail latency is also unmeasured: a creating request waits the full disk `request_timeout_ms` (30 s), so about 3 stalled attempts exhaust the 90 s window before the cap of 8 (`ensureBlobPresent` in `CasPartWriteTxn.cpp`).

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A large-blob Complete slower than 30 s is measured on RustFS and MinIO; outcome and tail latency recorded in the task
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
