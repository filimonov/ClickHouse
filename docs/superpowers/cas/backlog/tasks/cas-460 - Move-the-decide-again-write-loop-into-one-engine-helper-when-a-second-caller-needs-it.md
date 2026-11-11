---
id: CAS-460
title: >-
  Move the decide-again write loop into one engine helper when a second caller
  needs it
status: To Do
assignee: []
created_date: '2026-11-11 17:18'
labels:
  - 'area:write-path'
  - 'origin:review'
  - 'complexity:small'
  - 'risk:low'
dependencies:
  - CAS-368
priority: low
type: feature
ordinal: 564000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`Retry::resend_unresolved = false` (CAS-368) is honoured by every write verb in `writeLoop`, but only `casUpdateImpl` implements the matching decide-again loop. The caller mutates `attempts_already_sent` on a copy of the policy. `pauseBeforeRedecide` (`CA/Backend/CasRequests.cpp`) rebuilds a partial `WriteState`, counts a re-decide as a reissue, and ignores `attempt_spacing_ms`. A second caller would copy the loop, the counter arithmetic and the pause gate. `casAdmitEntry` still resends a fixed row and is the likely second caller. One caller exists today, so the helper is not built yet.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Trigger: a second call site needs `resend_unresolved = false`; until then the task stays open with no code
- [ ] #2 One engine helper owns the loop, `attempts_already_sent`, the pause and the counter choice; `Retry` stays immutable for callers
- [ ] #3 A re-decide is not counted as a reissue
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
