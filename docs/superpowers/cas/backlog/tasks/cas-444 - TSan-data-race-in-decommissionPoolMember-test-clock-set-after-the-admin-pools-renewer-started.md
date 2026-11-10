---
id: CAS-444
title: >-
  TSan data race in decommissionPoolMember: test clock set after the admin
  pool's renewer started
status: In Progress
assignee: []
created_date: '2026-11-10 22:23'
updated_date: '2026-11-10 22:56'
labels:
  - 'area:tests'
  - 'area:ci'
  - 'origin:ci'
  - 'touches:engine'
dependencies: []
priority: high
type: bug
ordinal: 547000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Unit tests (tsan) on PR 2805 @41d4a78eeb9 (run 42864462634, job unit_tests_tsan, log tmp/lanes/ci/pr2805_logs/unit_tests_tsan_41d4a78.log line 20655): ThreadSanitizer data race in CASDecommission.TombstoneRetryAfterTheLeaseDeadlineIsNotSent and the unit test binary aborted. Write: main thread, decommissionPoolMember -> Pool::setCasRequestNowFnForTest -> CasRequests::setNowFnForTest assigns lease_requests.now_ms (CasRequests.cpp:318) AFTER Pool::openForDecommission returned. Read: thread CasLeaseRenewer, started by openForDecommission via CasMountRuntime::startBackgroundWorkers (CasPool.cpp:974), calls now_ms in CasOperation::replace from MountLeaseRenewer::renewOn (CasServerRoot.cpp:1984). The post-open setters in Tools/CasDecommission.cpp (the block 'admin->setCasRequestNowFnForTest(drain_now_fn); admin->setCasRetrySleepForTest(drain_sleep_fn);' right after openForDecommission) are redundant: the same function already set config.boot_ms_fn and config.retry_sleep_fn before the open, and the Pool constructor builds mount_requests, lease_requests and gc_requests from them (CasPool.cpp:189-217). Only callers that pass drain fns are the gtests; InterpreterSystemQuery.cpp:1200 passes none. Same code on antalya-26.6 (origin PR 2574); TSan unit tests are skipped on most PRs of both branches, PR 2801 (26.6) passed once, so the race is interleaving-dependent. Fix: drop the two post-open setter calls (and the matching control_requests setters are fine: control_requests is constructed on the main thread before use). Land on antalya-26.6 first, port to 26.8.
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
2026-11-10: the fix already exists: https://github.com/Altinity/ClickHouse/pull/2801 (k-morozov, antalya-26.6, head 3963b4f40ec, open since 2026-11-05, no review yet, 28 commits behind the base). Its final diff is the same removal of the two post-open setters in Tools/CasDecommission.cpp, plus wiring config.retry_sleep_fn into ref_ledger in openForDecommission (the dropped setCasRetrySleepForTest also covered the ledger) and EXPECT->ASSERT in gtest_cas_gc_teardown_stop. Unit tests (tsan) green there (35 min). Its reds are unrelated: 01171_mv_select_insert_isolation_long and 00974_query_profiler on the tsan cas-s3 shards (rerun passed, flakes), Integration tsan 4/6 (docker pre-pull), tsan 5/6 test_cas_gcs token count (CAS-430), Stress tsan 'Table default.local_cold does not exist' on metadata load (Hybrid table 03646_hybrid_segment_pruning, same on PR 2812, base), CASAlter attach 3 (CAS-440), CASLightweightDelete 3 / CASS3CacheLightweightDelete 3 (CAS-340). Plan: merge 2801 into antalya-26.6, then cherry-pick onto the 26.8 port branch (PR 2805).

2026-11-10: PR 2801 merged into antalya-26.6 as 943249ecbb04daff64e556ef6403b639de7275b5 (22:41 UTC). Remaining: cherry-pick onto the 26.8 port branch (PR 2805); antalya-26.8 still has the post-open setters at Tools/CasDecommission.cpp:168-175 and the code there has diverged (standalone `requests` engine), so it is a hand port, not a plain pick. Review notes for the port: keep the one-line rule in Backend/CasRequests.h ('test setters belong to setup, before any operation runs'), fix the 'replace' wording in Tools/CasDecommission.h:67-70 and the 'never in unit tests' comment at Pool/CasPool.cpp:950-957.
<!-- SECTION:NOTES:END -->
