---
id: CAS-381
title: >-
  Close the terminal-latch admission window between a loss latch and mount
  re-arming
status: To Do
assignee: []
created_date: '2026-10-12 11:52'
labels:
  - 'area:mounts'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-misc.md
priority: high
type: bug
ordinal: 475000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Opening passes its check; another thread latches loss and trips the fence; arming clears `lost`; admission can succeed before arming rechecks the end. Highest direct ownership risk in wave 2; data loss not demonstrated. `CaDiskLifecycle` fuses the check and the arming. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-misc.md (finding 1). Found by the codex model-atomicity audit (wave 2) on 2026-10-12; code steps verified by codex, harm as marked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 model split shows the window
- [ ] #2 arming re-checks the terminal latch under the same lock that clears `lost`
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
