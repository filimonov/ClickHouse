---
id: CAS-438
title: >-
  03927_autopr_input_bytes_estimation_prewhere_filter: 170 s per query on the
  cas s3 storage lane under randomized settings
status: To Do
assignee: []
created_date: '2026-11-06 15:26'
updated_date: '2026-11-11 10:16'
labels:
  - cas
  - ci
  - 'theme:ci-stabilization'
dependencies: []
ordinal: 541000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2805 @a2147e55697, Stateless tests (amd_binary, cas s3 storage, parallel): timeout 1200 s. Not a hang: the 9 SELECTs over test.hits (8.87M rows, 1084 marks, parallel replicas x3) take 166-183 s each with the randomized settings and 0.03-1.2 s without them (query_log: tmp/lanes/ci/dl2805_sl/query_log.tsv.zst, slow run settings as SET statements: tmp/lanes/ci/03927_slow_settings.sql). The CI minimizer ran out of budget; the only run with a settings subset (172 of 173) was slow too. Settings that differ between every slow and every fast run include enable_filesystem_cache=0 (fast runs read test.hits from the local cache, slow runs from S3), merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.99, max_read_buffer_size=719992, merge_tree_min_bytes_per_task_for_remote_reading=16777216, filesystem_prefetches_limit=0, use_page_cache_for_object_storage=1. The same test passed in 4-7 s on the cas lanes of PR 2799, 2577 and 2802 (other random settings). Open question: does the CAS disk read path add anything over a plain s3 disk under these settings? To do: A/B locally (plain s3 disk vs cas disk, test.hits, the SET file), then either a CAS fix or a test tag/setting override proposal.
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
2026-11-06: fix on PR 2805 @8c26e8792eb (branch A): no-random-settings on 03800 and 03927, commit message links PR 2013 and the 2805 cas-lane job.log. Branch A also merged altinity/antalya-26.8 (0a4515c9f63, PR 2646). 2812 content already on the branch as 086efc95363, nothing to cherry-pick. 00165/00139/00038/00446 tags from PR 2013 deliberately not ported: no recorded reason for them.
<!-- SECTION:NOTES:END -->
