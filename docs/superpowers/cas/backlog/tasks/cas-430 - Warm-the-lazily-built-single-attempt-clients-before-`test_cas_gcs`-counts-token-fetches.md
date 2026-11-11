---
id: CAS-430
title: >-
  Warm the lazily built single-attempt clients before `test_cas_gcs` counts
  token fetches
status: To Do
assignee: []
created_date: '2026-11-04 16:13'
updated_date: '2026-11-11 10:16'
labels:
  - 'area:tests'
  - 'area:gcs'
  - 'origin:ci'
  - 'theme:ci-stabilization'
dependencies: []
ordinal: 533000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`test_cas_gcs::test_interleaved_ordinary_and_cas_operations_do_not_leak_mode_or_build_a_client` failed on https://github.com/Altinity/ClickHouse/pull/2574 (head 57c7944383b, Integration tests 7/8) with `assert 1 == 0`: one metadata-server token fetch inside the measured slice.

Root cause (logs in master/tmp/pr2574/it7/): the preceding test `test_a_permissive_service_does_not_change_what_cas_puts_on_the_wire` restarts the node (03:31:47, Ready 03:31:57); the failing test resets the token counter at 03:32:01 and its first `INSERT INTO t_cas_gcs_oauth` at 03:32:08 is the first blob write on the OAuth disk since the restart. `S3ObjectStorage::getSingleAttemptClient` caches single-attempt clones per timeout profile (attempt timeout, connect cap, adaptive flag); the mount builds only the control-plane clone, the write-profile clone is built at the first blob publication (`S3Client: S3 client for disk 'cas_gcs_oauth' initialized with s3_retry_attempts: 0` at 03:32:08.684, first PUT 4 ms later). Each clone owns a `gcp_oauth_client` with its own token cache, so the fetch counts. The docstring's premise ("the base and single-attempt clients already exist by this point, having been built during the mount and the first conditional write") is false right after a restart. The same code passed on #2567's final run and on the previous #2574 head: timing, not PR C.

Fix in the test, not in the product (lazy per-profile clones are bounded and intended; the key set comes from request budgets, so eager construction at disk start has nothing to enumerate): run one warm-up round of the same INSERT / DROP CACHES / SELECT workload on both disks BEFORE `_next_seq()` and `_reset_token_fetches()`, then measure the three rounds. Anything built lazily by these operations appears in the warm-up; a fetch inside the measured rounds then means the request mode built a client, which is what the test guards. Update the docstring paragraph accordingly. Removing the neighbour's restart does not fix it (any order with a restart reproduces); a restart at the start of this test adds ~10 s under ASan and still needs the warm-up.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A warm-up round identical to a measured round runs before `_next_seq()` / `_reset_token_fetches()`; the three measured rounds are unchanged
- [ ] #2 The docstring no longer claims the clients were built by the mount and the first conditional write; it names the warm-up round
- [ ] #3 Reproduction before the fix: with `node.restart_clickhouse()` inserted at the start of the test it fails with `1 == 0`; after the fix it passes with and without the restart (`python -m ci.praktika run integration --test test_cas_gcs`, ASan build)
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
