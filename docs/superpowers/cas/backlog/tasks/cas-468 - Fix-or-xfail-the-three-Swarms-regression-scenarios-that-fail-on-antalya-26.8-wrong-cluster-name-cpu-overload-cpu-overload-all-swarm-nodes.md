---
id: CAS-468
title: >-
  Fix or xfail the three Swarms regression scenarios that fail on antalya-26.8
  (wrong cluster name, cpu overload, cpu overload all swarm nodes)
status: To Do
assignee: []
created_date: '2026-11-12 21:36'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: low
type: bug
ordinal: 573000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Not CAS: the jobs mount no CAS disk. Scenarios: /swarms/feature/cluster discovery/check cluster discovery with wrong cluster name (Code: 279 ALL_CONNECTION_TRIES_FAILED, exitcode 23 where 33 is expected, swarms/tests/cluster_discovery.py:322); /swarms/feature/node failure/cpu overload; /swarms/feature/node failure/cpu overload all swarm nodes (AssertionError after about 5 min). Jobs 131374909292 (aarch64) and 131386694514 (release). Same three on base antalya-26.8 run 43246617309 (jobs 131295799442, 131294955447) and on run 43091569224. PR https://github.com/Altinity/ClickHouse/pull/2831 @d8cbdfae3ec, run 43248330323; triage tmp/prbundle/wave1/ci/triage.md. CAS-450 AC 7 needs a public issue link.
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
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: task notes: the same three Swarms scenarios fail on base antalya-26.8 runs 43246617309 and 43091569224; CAS-450 note 2026-11-13 says CAS-468 did not recur in run 43379266824. Changes: size unset -> small (S): fix or xfail of three scenarios; cause not investigated; risk set to low; confidence set to plausible; impact:operability; type unset -> bug: scenarios fail on the base branch, not CAS. Size: S. Would settle it: a current antalya-26.8 run showing whether the three scenarios still fail (they did not recur on 2026-11-13); a public issue link (CAS-450 AC7).
---
<!-- COMMENTS:END -->
