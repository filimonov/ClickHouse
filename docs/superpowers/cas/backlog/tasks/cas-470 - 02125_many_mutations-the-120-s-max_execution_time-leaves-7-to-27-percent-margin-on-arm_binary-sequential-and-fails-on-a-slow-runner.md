---
id: CAS-470
title: >-
  02125_many_mutations: the 120 s max_execution_time leaves 7 to 27 percent
  margin on arm_binary sequential and fails on a slow runner
status: To Do
assignee: []
created_date: '2026-11-12 21:36'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
  - 'complexity:trivial'
  - 'upstream:not-found'
  - groomed-20261121
  - grooming-20261121-changed
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - 'risk:low'
  - 'confidence:solid'
dependencies: []
priority: low
type: chore
ordinal: 575000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Not CAS: the lane mounts no CAS disk. Job 131345853046: optimize table many_mutations final hit TIMEOUT_EXCEEDED at 120000 ms, test FAIL after 267 s. Base: lane green, the test took 111 s (run 43246617309 job 131295790510) and 87 s (run 43091569224 job 130921282554). The PR job ran 1.65 times longer overall. One occurrence. PR https://github.com/Altinity/ClickHouse/pull/2831 @d8cbdfae3ec, run 43248330323; triage tmp/prbundle/wave1/ci/triage.md.
<!-- SECTION:DESCRIPTION:END -->

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
created: 2026-11-21 08:09
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: tests/queries/0_stateless/02125_many_mutations.sh:21 and :48 still pass --max_execution_time 120; one recorded failure. Changes: size unset -> trivial (XS): one setting in one test; type unset -> chore: test-only timeout margin, no behaviour change. Size: XS. Links: Upstream PR 78123 disabled 02125_many_mutations* for Replicated database (too slow): different case, same slowness class.. Would settle it: failure frequency of 02125_many_mutations on arm_binary sequential in master history. Upstream: not-found.
---

author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21 (addendum): added risk:low, confidence:solid.
---
<!-- COMMENTS:END -->
