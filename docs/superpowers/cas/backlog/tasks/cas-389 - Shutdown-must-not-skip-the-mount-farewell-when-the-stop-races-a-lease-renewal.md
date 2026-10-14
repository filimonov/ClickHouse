---
id: CAS-389
title: Shutdown must not skip the mount farewell when the stop races a lease renewal
status: To Do
assignee: []
created_date: '2026-10-14 17:03'
labels:
  - 'area:mounts'
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2567'
priority: high
type: bug
ordinal: 487000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Cause 1: the stop flag is read before admission. Fixed in PR B commit 1a9f1fe5bcd; present on the release line since 37c9bd4356b; cherry-pick candidate.

Cause 2: a stop after the renewal write was sent ends the renewer terminally and skips the farewell. Fail-safe, but the mount is left unfarewelled. Options:
(a) stopBackgroundWorkers waits for an in-flight renewal (recommended);
(b) postCommit refuses only on fence loss;
(c) read the slot before the farewell;
(d) document it. Interim (d) is in PR B's two tests, which tolerate the race.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Option (a) is implemented with a failing-first test: a 20 ms delay inside the /mount write reproduces the skipped farewell in 9 of 30 runs
- [ ] #2 The PR B test tolerances for this race are removed
- [ ] #3 The cause-1 commit is cherry-picked to the release line
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
