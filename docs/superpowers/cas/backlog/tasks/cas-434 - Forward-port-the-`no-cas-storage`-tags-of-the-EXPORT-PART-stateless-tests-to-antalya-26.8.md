---
id: CAS-434
title: >-
  EXPORT PART limits test exceeds its wait budgets on sanitizer cas-s3 lanes
  (antalya-26.8)
status: To Do
assignee: []
created_date: '2026-11-05 13:00'
updated_date: '2026-11-11 10:16'
labels:
  - 'area:tests'
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
milestone: m-8
dependencies: []
references:
  - >-
    tests/queries/0_stateless/03572_export_merge_tree_part_limits_and_table_functions.sh
priority: medium
type: bug
ordinal: 537000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`03572_export_merge_tree_part_limits_and_table_functions` exceeds its two time budgets on the sanitizer `cas s3 storage` lanes of antalya-26.8: the 60 s `wait_for_exports` poll and the 300 s client receive timeout on `OPTIMIZE TABLE ... FINAL`. It passes in 12-20 s on every non-sanitizer CAS lane.

The four EXPORT PART tests run on CAS lanes on purpose: `cc73c9b6eea` tags them `no-cas-storage` on antalya-26.6, and `5a1ffb222a5` ("Allow every partition command, EXPORT PART included, on a CAS disk") removed the tags on antalya-26.8.

Observed: `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)` on https://github.com/Altinity/ClickHouse/pull/2773 (sha 307f55348011ccfc75290a484e5ce3dff9ca761a, 2026-11-05): FAIL after 351 s, `Timeout waiting for exports to complete after 60s`, 2 of 4 exports in `part_log`, `system.exports` empty. The same test passed on https://github.com/Altinity/ClickHouse/pull/2730 shard 2/2 the same night. https://github.com/Altinity/ClickHouse/pull/2805 run 42921139681: `FAIL` on msan 2/3, `BROKEN` on asan 2/2 and tsan 2/2.

The test is already `BROKEN` on the tsan lanes without CAS (MasterCI run 39839856866) and `tests/broken_tests.yaml` covers it for tsan only. Measurements and options are in the implementation notes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The limits test does not report FAIL on the asan, msan and tsan cas-s3 lanes of antalya-26.8: either its budgets fit a loaded sanitizer lane, or `tests/broken_tests.yaml` covers the sanitizers with a recorded reason
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
2026-11-06: 26.8 removed the four tags on purpose in 5a1ffb222a5 ("Allow every partition command, EXPORT PART included, on a CAS disk", PR 2647 series), so the tags are not forward-ported with the lane-parity branch; AC#1 needs the wait-budget decision.

2026-11-11 PR https://github.com/Altinity/ClickHouse/pull/2805 @3fa3a01203b (run 42921139681): `03572_export_merge_tree_part_limits_and_table_functions` failed on msan 2/3 (481.59 s, `Waiting for exports to complete (timeout: 60s)`, reference diff) and timed out at 600 s on asan 2/2. The test still carries only `no-fasttest` on the PR head.

2026-11-11 correction: no port gap, and the cause is lane time, not CAS behaviour.

Tags: `cc73c9b6eea` reached antalya-26.8 with `c770be91e83`; `5a1ffb222a5` removed the four tags on purpose. The title and the "cherry-pick" option are stale. The note above also overstates run 42921139681: the test is `BROKEN` (not counted) on asan 2/2 (600.09 s) and tsan 2/2 (613.64 s); msan 2/3 (481.59 s) is the only `FAIL`.

Same run, no sanitizer: OK in 11.8 s on `amd_binary, cas storage`, 16.1 s on `amd_binary, cas s3 storage`, 19.7 s on `arm_binary, cas s3 storage`; plain lanes 10.8 s (`amd_debug`) and 16.4 s (`arm_binary`).

Without CAS on antalya-26.8 (MasterCI run 39839856866, sha ab9c4003d72ad373f88f9695b4ceb7845a919b7a): asan 79.5 s OK, msan 240.5 s OK, tsan 359.7 s `BROKEN`, tsan `s3 storage` 904.8 s `BROKEN`. Both tsan lanes show the same two symptoms as the cas-s3 lanes: `Timeout waiting for exports to complete after 60s`, and `OPTIMIZE TABLE ... FINAL` over the 300 s client receive timeout. `tests/broken_tests.yaml` lists the test for tsan only (`message: 'timeout: 60s'`, added by `5aaf212c0d8`), so msan is the lane that reports `FAIL`.

Sanitizer cas-s3 lanes of run 42921139681:
- asan 2/2, position 5587/6933: INSERTs 39 s and 31 s, `OPTIMIZE FINAL` hit the 300 s receive timeout.
- tsan 2/2, position 4761/6933: INSERTs 138 s and 76 s, `OPTIMIZE FINAL` ~3 min, big exports finished 94 s and 175 s after the small ones.
- msan 2/3, position 1609/4640: `OPTIMIZE FINAL` ~4 min 20 s, both big exports still running after 120 s.
Exported data was complete whenever the exports finished.

Local discriminator (ASan binary of `ad04f95e441`, this test alone on an idle machine, `python3 -m ci.praktika run ... --test 03572_export_merge_tree_part_limits_and_table_functions`): `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)` OK in 36.61 s; `Stateless tests (amd_asan_ubsan, distributed plan, parallel, 2/2)` (`--workflow MasterCI`) OK in 45.58 s. EXPORT PART from a CAS part is not slower than from a plain part under ASan. The 600 s in CI comes from the loaded cas-s3 sanitizer lane (CAS-432); the hot component inside `OPTIMIZE FINAL` there is not identified, the timed-out jobs uploaded no server logs.

Options: (a) extend the `tests/broken_tests.yaml` entry to msan and asan, as the base branch already does for tsan; (b) raise the `wait_for_exports` budget (does not cover the `OPTIMIZE FINAL` receive timeout); (c) shrink the 2 x 4194304-row input. Re-adding `no-cas-storage` would hide a supported path that passes on every non-sanitizer CAS lane.
<!-- SECTION:NOTES:END -->
