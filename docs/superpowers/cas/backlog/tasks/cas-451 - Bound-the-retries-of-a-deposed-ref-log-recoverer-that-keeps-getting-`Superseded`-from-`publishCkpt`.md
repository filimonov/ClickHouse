---
id: CAS-451
title: >-
  Bound the retries of a deposed ref-log recoverer that keeps getting
  `Superseded` from `publishCkpt`
status: To Do
assignee: []
created_date: '2026-11-11 11:49'
labels: []
dependencies:
  - CAS-366
priority: low
ordinal: 554000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After CAS-366, `publishCkpt` returns `CkptPublishOutcome::Superseded` when a contribution names a frontier that a later writer epoch already moved past. Callers treat it like `FencedOut`: the lane stays in recovery and the write answers retry-later. A deposed mount whose lease check has not fired yet repeats recovery and gets `Superseded` each time. Nothing bounds that loop except the lease check. Decide whether a repeated `Superseded` should latch the mount fault (as a lost lease does) or stay as it is, and say so in the code.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A written decision: latch the mount fault after `Superseded`, or keep retry-later until the lease check fires, with the reason
- [ ] #2 If the decision is to latch: a gtest drives recovery into `Superseded` and sees the mount fault; no further catalog or `_ckpt` writes follow
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
