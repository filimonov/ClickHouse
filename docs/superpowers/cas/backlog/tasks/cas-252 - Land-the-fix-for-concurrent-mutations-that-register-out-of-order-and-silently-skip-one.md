---
id: CAS-252
title: >-
  Land the fix for concurrent mutations that register out of order and silently
  skip one
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-09-28 15:50'
labels:
  - 'area:upstream'
  - 'complexity:small'
  - 'risk:medium'
  - 'touches:upstream-code'
  - 'confidence:solid'
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
First recorded: 2026-09-26 (7a99f6b7825, by 'mutation-registration-race')

Issue #2331 (open, reopened in comments): 23 failures in 45 days, every one with rows surviving (1 to 51 extra rows), one on upstream `clickhouse/clickhouse-server:head-alpine`, none in 520 runs of 23.3-25.8, so the race is a 26.x regression; CAS raises the hit rate about tenfold.

2026-09-28: reported as Altinity issue https://github.com/Altinity/ClickHouse/issues/2331 (2026-09-09, CI: cas_lightweight_delete survivors, also without CAS) and upstream https://github.com/ClickHouse/ClickHouse/issues/122083 (2026-09-24, confirmed on master). Sibling upstream race (heavyweight mutation vs in-flight lightweight UPDATE): issue #116686, open PR https://github.com/ClickHouse/ClickHouse/pull/119155 (bounds selectPartsToMutate by in-flight Op::Update blocks; a maintainer suggested covering our case there). Reproduced again from CI logs on PR #2440 (tmp/cas95/rca_2440_cas_reds.md item 3): mutation_50 registered 1.1 ms after mutation_51, one row survived.

2026-09-28: design rev.9 final (docs/superpowers/specs/2026-09-28-mergetree-uncommitted-version-boundary-design.md, six codex rounds in docs/superpowers/reports/2026-09-28-cas-252-codex-reviews/). Target order (owner): upstream first, on fresh upstream/master with PR #119155's two real commits cherry-picked (8db4dc2b953, 4398d337461), in worktree mt-pending-versions; antalya-26.6 afterwards. Upstream master already commits the MOVE destination under the parts lock (PR #108922), so the MOVE writer of the rule is not needed there; antalya-26.6 gets the backport as a prerequisite instead (task created).
<!-- SECTION:NOTES:END -->
