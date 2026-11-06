---
id: CAS-436
title: >-
  Port the CAS PRs that landed on antalya-26.6 after the PR 2540 cut to
  antalya-26.8
status: To Do
assignee: []
created_date: '2026-11-06 02:13'
updated_date: '2026-11-06 08:37'
labels:
  - 'area:backend'
  - 'area:ci'
dependencies: []
priority: high
ordinal: 539000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2540 ported antalya-26.6 CAS as of 2026-10-14 (after PR 2437 plus 2509-2511). Missing on antalya-26.8 since then: 2512, 2552, 2513, 2516, 2555, 2517, 2566 (via the antalya-26.6-20261008 branch, PR 2644) and 2567, 2594, 2574, 2573 (2026-11-04). Verified by patch-id and by added-line presence (tmp/lanes/port_score.sh in the master worktree: 0-24% of each PR's added source lines exist on 26.8; the ported control PR 2483 scores 100%). Local branch port/antalya-26.8/cas-from-26.6 (worktree antalya-26.8-pr-2159) carries one cherry-pick per PR with the conflict notes in the commit messages; analysis in tmp/lanes/REPORT.md. Not ported on purpose: the EXPORT PART no-cas-storage tags (26.8 removed them in 5a1ffb222a5, see CAS-434). Open 26.6 PRs 2577 and 2570 follow once merged.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The eleven PRs are on antalya-26.8 with the CAS* gate and the ASan cas-s3 lane green
- [ ] #2 PRs 2577 and 2570 are ported after they merge into antalya-26.6
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
2026-11-06: port/antalya-26.8/cas-from-26.6 @b8dfb399067 (15 commits: 11 picks + 4 fix-ups 78de57baf9d, 4e5fc3c5348, c7986555568, b8dfb399067) builds in release; CAS* gate 2904 passed, 0 failed, 2 disabled (antalya-26.8-pr-2159/build/test_cas_gate_port_a.log). ASan lane not run locally (disk full). Not pushed.

2026-11-06: PR https://github.com/Altinity/ClickHouse/pull/2805 (head b8dfb399067, base antalya-26.8) opened; CAS regression job ticked, ASan lanes on.

2026-11-06: open 26.6 PRs checked against branch A: 2570 and 2498 apply clean, 2577 (test .load() reads) and 2801 (include list) one trivial conflict each, drafts 2534/2542 conflict with the GC/sweep code (also on 26.6). Scratch branch port/antalya-26.8/cas-from-26.6-pending @35653f83562 (2570, 2498, 2577, 2801 on top of b8dfb399067) builds; CAS* gate 2913 passed. Not pushed; re-apply after 2805 merges and the source PRs settle.
<!-- SECTION:NOTES:END -->
