---
id: CAS-302
title: >-
  Bring the CAS fixes merged only into antalya-26.6 back into cas-gc-rebuild
  (PRs #2393, #2384, #2349, #2326)
status: To Do
assignee: []
created_date: '2026-09-26 12:37'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:ci'
  - 'area:gc'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
  - 'needs:decision'
  - groomed-20261121
  - 'groom-20261121:obsolete'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2393'
  - 'https://github.com/Altinity/ClickHouse/pull/2384'
  - 'https://github.com/Altinity/ClickHouse/pull/2349'
  - 'https://github.com/Altinity/ClickHouse/pull/2326'
priority: high
type: chore
ordinal: 381000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Several CAS pull requests were merged into `altinity/antalya-26.6` and never came back to `cas-gc-rebuild`: #2393 (do not probe listing keys as JSON), #2384 (cap the RustFS blocking-thread pool on the CAS-S3 lanes), #2349, and #2326 (GC scheduler stop/join race, tracked as CAS-166). The two branches drift silently; the grooming of 2026-09-26 found several backlog claims that were true on one branch only. Bring the merged fixes over (merge or cherry-pick), then add a periodic check that every CAS-labelled PR merged into antalya-26.6 is an ancestor of cas-gc-rebuild.

Provenance: GitHub reconciliation 2026-09-26 (unit u17); no backlog task tracked the branch sync.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every CAS-labelled PR merged into antalya-26.6 is an ancestor of cas-gc-rebuild (list produced by `gh pr list --label CAS --state merged` and `git merge-base --is-ancestor`)
- [ ] #2 CAS-166 is closed by the same sync
- [ ] #3 A documented one-line check exists that reports the next drift
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: obsolete (medium). Evidence: CAS code now ships from antalya-26.6 and its 26.8 port: CAS-436 notes show 26.6 CAS PRs ported to antalya-26.8 (db0431e95cd) and CAS-222 notes show #2349 already on 26.8; the checked 26.8 branch has the MemoryWorker fix (src/Common/MemoryWorker.cpp:583); project history says CAS code branches from antalya, not cas-gc-rebuild. Changes: label needs:decision. Links: superseded in practice by CAS-436 (26.6 to 26.8 port); related CAS-222, CAS-166. Would settle it: owner confirmation that cas-gc-rebuild is no longer a code delivery branch (it still carries specs and docs); AC3 (a one-line drift check) could be kept as a separate small task for 26.6 vs 26.8. Owner question: Is cas-gc-rebuild retired as a code branch, so this sync can be closed?
---
<!-- COMMENTS:END -->
