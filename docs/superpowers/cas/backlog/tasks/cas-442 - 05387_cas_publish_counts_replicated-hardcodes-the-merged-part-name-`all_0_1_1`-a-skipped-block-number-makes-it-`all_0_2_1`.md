---
id: CAS-442
title: >-
  05387_cas_publish_counts_replicated hardcodes the merged part name
  `all_0_1_1`; a skipped block number makes it `all_0_2_1`
status: To Do
assignee: []
created_date: '2026-11-06 22:01'
updated_date: '2026-11-06 22:13'
labels:
  - cas
  - ci
  - test
dependencies: []
priority: medium
type: bug
ordinal: 545000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2805 @8c26e8792eb, job `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 1/2)`: reference `replicated_merge 1 0 3`, got `replicated_merge 0 0 3`. part_log of the run (database test_c6x3o3i6p0w4): r1 NewPart all_0_0_0, r2 DownloadPart all_0_0_0, r1 NewPart all_2_2_0 (block number 1 was skipped), r2 DownloadPart all_2_2_0, r1 MergeParts all_0_2_1 (rows 2000). The merge was published once under its final name, as the test asserts, but the test counts `ref_name = 'all_0_1_1'`. 3 reruns passed. Fix: take the merged part's name from system.parts (active, level > 0) instead of the literal, or match `ref_name LIKE 'all_%_1'`. The test lives on antalya-26.6 and on the PR 2805 port branch. Log: https://altinity-build-artifacts.s3.amazonaws.com/PRs/2805/8c26e8792eb4d675d6cfa59b06f8e31b38471008/stateless_tests_amd_asan_ubsan_cas_s3_storage_parallel_1_2/part_log.tsv.zst
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The test derives the merged part name instead of hardcoding it, on antalya-26.6 and on the 26.8 port
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
2026-11-06: fixed on the PR 2805 branch as 4810e687c92 (system.parts name instead of the literal); 5/5 local runs OK on a standalone 26.8 stand. The antalya-26.6 copy still has the literal.
<!-- SECTION:NOTES:END -->
