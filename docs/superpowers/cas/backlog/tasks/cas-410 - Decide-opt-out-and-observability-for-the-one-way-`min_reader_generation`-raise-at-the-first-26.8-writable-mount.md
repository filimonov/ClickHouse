---
id: CAS-410
title: >-
  Decide opt-out and observability for the one-way `min_reader_generation` raise
  at the first 26.8 writable mount
status: To Do
assignee: []
created_date: '2026-10-22 07:59'
labels:
  - 'area:mounts'
  - 'area:formats'
  - 'area:observability'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:review'
milestone: m-7
dependencies:
  - CAS-346
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp
  - docs/en/antalya/cas/configuration.md
priority: medium
type: design
ordinal: 513000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8) a writable mount by a 26.8 server raises the pool's `min_reader_generation` from 1 to 2 once, before any owner `Move` exists (`Pool/CasPool.cpp` `Pool::open`); the setting `cas_ref_move_enabled` that gated it in the draft was removed. After the raise 26.6.x refuses to mount; a 26.6.x node that is already mounted keeps running but cannot re-mount after a lost lease or fence. Reviewers (UX, compat, quality) rated the lack of a switch and of a signal as major; the owner kept the one-way raise (upgrade order: stop every 26.6.x first, documented in `docs/en/antalya/cas/configuration.md`). Open decisions: (1) a lazy raise at the first `Move` append instead of at mount; (2) an operator-visible signal: `system.cas_mounts` column, a ProfileEvent/log line (today only the log line 'min_reader_generation raised 1 -> 2'); (3) whether a read-only 26.8 mount should ever raise (it does not today).

Provenance: umbrella review of the owner-Move series, 2026-10-22 (majors 2, perspectives UX/compat/quality); `tmp/clean/umbrella_review.md` in worktree antalya-26.8-pr-2159.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Owner decision recorded here: mount-time vs first-Move raise, and whether a switch exists
- [ ] #2 The raise is visible without reading logs (`system.cas_mounts` or a counter), with a test
- [ ] #3 `docs/en/antalya/cas/configuration.md` upgrade section matches the decision
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
