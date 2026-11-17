---
id: CAS-252
title: >-
  Land the fix for concurrent mutations that register out of order and silently
  skip one
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:upstream'
  - 'complexity:small'
  - 'risk:medium'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'theme:ci-stabilization'
milestone: m-5
dependencies: []
references:
  - src/Storages/StorageMergeTree.cpp
  - >-
    https://github.com/Altinity/ClickHouse/tree/feature/antalya-26.6/fix-mutation-registration-race
  - 'https://github.com/Altinity/ClickHouse/pull/2300'
priority: high
type: bug
ordinal: 317000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`StorageMergeTree::prepareMutationEntry` allocates the block number and writes `mutation_N.txt` outside
`currently_processing_in_background_mutex`; registration happens later in `addPreparedMutationEntry`
(`src/Storages/StorageMergeTree.cpp:889`, `:927`). `selectPartsToMutate` (`:1775`) bounds the range only by the map's end, so a
task running between two registrations applies the later mutation alone and the part never gets the earlier one, while
`is_done = 1`. Two concurrent lightweight `DELETE`s are enough. Not CAS-specific; CAS only widens the window (198 us vs 21 us).
Upstream master has the same bound. Unfixed on both cas-gc-rebuild and antalya-26.6.
A fix exists on `feature/antalya-26.6/fix-mutation-registration-race` (`c4b2ef2c1e6` + `27758e5de46`): bound by the lowest
in-flight mutation block, postpone reason `EARLIER_MUTATION_NOT_REGISTERED`, failpoint `mt_pause_before_mutation_registration`,
test `05027_mutations_register_out_of_order.sh`. It was reverted on the CAS branches by user decision (2026-09-09): it ships
as its own pull request, not inside CAS.

Provenance: BACKLOG/replication.md#mutation-registration-race; observed in CI as the `cas_lightweight_delete` survivors of PR #2300 and reproduced by 05027 (not a review-only hypothesis); revert reason = user decision 2026-09-09 to keep generic fixes off the CAS branches (reverts c8d14a95d3a, a04846cfdeb carry no reason). Verified 2026-09-26 against 8b87aa15d21 (cas-gc-rebuild) and 8d62c314ec1 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A pull request with the fix and `05027_mutations_register_out_of_order.sh` targets antalya-26.6 and passes CI
- [ ] #2 An upstream ClickHouse pull request or issue with the same fix and test is open
- [ ] #3 The test fails without the fix and passes with it
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
2026-10-01: occurrence on PR #2463 cas_lightweight_delete_4 (release x86): 'without overlap' scenarios 251 vs 250, 3 vs 0; tmp/ci-2463/rca.md

2026-11-11 PR https://github.com/Altinity/ClickHouse/pull/2805 @3fa3a01203b (run 42921139681): `RegressionTestsAarch64 / CASS3CacheLightweightDelete (4)` failed `/lightweight delete/concurrent delete/MergeTree/random delete entire table`: `concurrent_delete.py:285` read 1 row where it expects 0. Same surplus-row signature as CAS-333; the server log of this run was not read.

2026-11-11: PR 2805 head `2f89c5db182`, CI run 42999688750 (triage: tmp/pr2805/run_42999688750/triage.md). Release `cas_s3_cache_lightweight_delete_4`: 2 leaf scenarios failed, `/lightweight delete/concurrent alter and delete/AggregatingMergeTree/concurrent clear update and delete` and `/lightweight delete/concurrent delete/MergeTree/random delete half of the table`; `concurrent_alter_and_delete.py:341`: `count(*)` returned 1, expected 0 after acknowledged deletes. The first scenario was not seen before; same symptom as CAS-333.

2026-11-12: PR https://github.com/Altinity/ClickHouse/pull/2831 @d8cbdfae3ec, run 43248330323 (triage: tmp/prbundle/wave1/ci/triage.md). job 131386699125 (release CASS3CacheLightweightDelete (4)): /lightweight delete/concurrent alter and delete/MergeTree/concurrent clear update and delete, concurrent_alter_and_delete.py:341, count(*) = 550, expected 500 after the 60 s retry. First MergeTree variant of this scenario; same shard fails on base 43246617309 (job 131294967158) in concurrent delete/ReplacingMergeTree/concurrent delete without overlap. No server log in the console log.

2026-11-17: CI run 44084349150 on 44fc40023c1: `RegressionTestsAarch64 / CASS3CacheLightweightDelete (4)` failed on one leaf, `/lightweight delete/concurrent delete/VersionedCollapsingMergeTree/concurrent delete without overlap`; Release and non-cache twins green. Looks like this class; the mutation order was not read from the logs.
<!-- SECTION:NOTES:END -->
