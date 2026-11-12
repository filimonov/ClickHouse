---
id: CAS-466
title: >-
  Fix hang of gtest `CASRefWriterSnapshotPublish.C4InFlightGateAdmitsAtMostOne`
  when the setup publisher backs off on a `Writing` lane
status: To Do
assignee: []
created_date: '2026-11-11 21:46'
updated_date: '2026-11-12 16:55'
labels:
  - ci
  - 'area:tests'
  - 'origin:ci'
  - 'theme:ci-stabilization'
dependencies: []
references:
  - src/Disks/tests/gtest_cas_ref_writer.cpp
  - tmp/prbundle/pr2/logs/hang_t6_gate_C4InFlightGate_tail.log
priority: medium
type: bug
ordinal: 570000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The test hangs forever at `backend->awaitBlockEntered()` (`src/Disks/tests/gtest_cas_ref_writer.cpp:3306`, helper at `:839` has no deadline).

Root cause (test-only, read from the code at `e06224820c0`):
1. Setup `publishEmptyPart` commits two ref txns (`precommitAdd`, `promote`). With `snapshot_log_count_threshold = 0` the first commit dispatches a detached publisher.
2. If that publisher takes `state_mutex` while the second append holds the lane in `Writing`, it logs `refusing snapshot publication while the append lane is not Ready (state 1)` and arms the 200 ms backoff (`advancePublishBackoff`, `snapshot_publish_backoff_initial_ms`).
3. The second commit's `maybeScheduleSnapshotPublish` is refused by the backoff deadline. `waitForSnapshotPublishSettleForTest` returns at once with `pending_snapshot_publishes == 0` and an uncompacted tail. The test comment `tail is compacted` is false on this path.
4. The mutator's `dropRef` runs both of its triggers (pre-append in `appendRefOps`, post-commit in `commitRefChunk`) inside the same 200 ms. Neither dispatches. No `_snap/` PUT ever happens.

Not a product defect: in a server the next read or write after the backoff dispatches the publisher. CAS-148 covers the warning itself.

Seen once in several dozen `CAS*` gate runs on 2026-11-11 (release build, `unit_tests_dbms --gtest_filter=CAS*`, host under load). Alone the test passed 200 of 200.

Same shape to check: the other six `awaitBlockEntered` callers in `gtest_cas_ref_writer.cpp` and every test that reads `waitForSnapshotPublishSettleForTest` as "tail compacted" after a multi-txn setup (`gtest_cas_ref_writer.cpp` 20 uses, `gtest_cas_ref_snapshot_publish_ordering.cpp` 19, `gtest_cas_detached_work.cpp` 6, `gtest_cas_ref_chunked_flush.cpp` 4).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The test cannot hang: `RefWriterTestBackend::awaitBlockEntered` is bounded and fails loudly, as `cas_test_helpers.h:2575` already does
- [ ] #2 The setup reaches a compacted tail on every interleaving (for example `snapshot_publish_backoff_initial_ms = 0` in this test, or a drain helper that asserts `newestPublishedSnapshotIdForTest`)
- [ ] #3 A forced interleaving (publisher runs while the lane is `Writing`) shows the old test hanging or failing and the fixed one passing
- [ ] #4 The sibling callers listed in the description are checked and the result is recorded
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
2026-11-11: found while gating the namespace-drop retry-policy commit of the late-request bundle (worktree cas-pr1, base e06224820c0). The hung process had 35 sleeping threads and 25 s of CPU after 7 minutes. Related: CAS-148. A test-only fix is queued for the fix wave of that branch (tmp/prbundle/pr2/fixwave/batch.md).

2026-11-12. The hang is fixed at `fa1b89d1529` by `50dbaee3a69` and `f1bdb36d374` (both in `gtest_cas_ref_writer.cpp`). Left for wave 1 of the 26.8 bundle: AC #4, the check of the sibling tests that set `snapshot_log_count_threshold = 0` (two in `gtest_cas_detached_work.cpp`, two in `gtest_cas_ref_snapshot_publish_ordering.cpp`, one in `gtest_cas_ref_chunked_flush.cpp`, two in `gtest_cas_ref_writer.cpp`).
<!-- SECTION:NOTES:END -->
