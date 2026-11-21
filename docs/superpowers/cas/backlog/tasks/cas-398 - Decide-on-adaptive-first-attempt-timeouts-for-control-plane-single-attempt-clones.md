---
id: CAS-398
title: >-
  Decide on adaptive first-attempt timeouts for control-plane single-attempt
  clones
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:backend'
  - 'area:upstream'
  - 'needs:decision'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: low
type: design
ordinal: 496000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Control-plane single-attempt clones still run under short adaptive first-attempt timeouts, the same class #2555 fixes for blobs. Two overlapping knobs result: the `SingleAttempt` retry profile and the new creation flag, with different verb scope and a second clone when both are set. Either disable adaptive timeouts for every clone (drops `configured_timeouts`, the clone-key tuple and the `hasSingleAttemptClientForTest` edit) or document why control-plane keeps them.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded; code or docs match it
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
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1: hasSingleAttemptClientForTest(request_timeout_ms, connect_timeout_cap_ms) still keys clones by timeout pair (S3ObjectStorage.h:227, .cpp:1344); no decision recorded in the task. Changes: type unset -> design: the task asks for a decision. Pointers stale: configured_timeouts not found in cas-pr1 src/Disks or src/IO/S3. Would settle it: owner decision, and whether configured_timeouts (named in the task) still exists: grep finds no such identifier in src/Disks or src/IO/S3. Owner question: Disable adaptive first-attempt timeouts for every single-attempt clone, or keep them for the control plane and document why?
---
<!-- COMMENTS:END -->
