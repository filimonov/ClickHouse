---
id: CAS-464
title: >-
  Find why `04078_native_protocol_input_validation` times out at stage 5 on the
  TSan cas-s3 stateless lane
status: To Do
assignee: []
created_date: '2026-11-11 21:18'
updated_date: '2026-11-21 08:25'
labels:
  - cas
  - ci
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
  - 'confidence:speculative'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2805'
  - 'https://github.com/Altinity/ClickHouse/actions/runs/42999688750'
priority: low
type: research
ordinal: 568000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2805 head `2f89c5db182`, CI run 42999688750, job `Stateless tests (amd_tsan, cas s3 storage, parallel, 1/2)`: `04078_native_protocol_input_validation` failed with `AssertionError: stage=5: timeout, result:` at `tests/queries/0_stateless/04078_native_protocol_input_validation.py:243`. Praktika reran it 94 times with the same randomized settings and all passed. Not seen in run 42921139681 on `3fa3a01203b`. Not determined: whether the server answered the invalid-stage packet late under TSan load or not at all. No server log was read.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The server log of the failed run says whether the stage-5 packet was answered and when
- [ ] #2 The cause is named: slow lane, test timeout too short for TSan, or a server defect; a fix or a follow-up task exists for it
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
Evidence: tmp/pr2805/run_42999688750/triage.md (NEW item 1); job log tmp/pr2805/run_42999688750/logs/stateless_tests_amd_tsan_cas_s3_storage_parallel_1_2.log. Listed under the umbrella CAS-450.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: tmp/prbundle/wave1/ci/triage.md (PR 2831 run 43248330323): 04078 shows only [ OK ] lines in seven logs; the only failure is the single occurrence in run 42999688750; no server log read, cause not established. Changes: confidence set to speculative; type bug -> research: task asks to find a cause; no defect is shown yet; priority medium -> low: one occurrence in run 42999688750, 94 praktika reruns pass, not seen in two later runs. Would settle it: the server log of run 42999688750 job amd_tsan cas s3 1/2 for the stage-5 packet (AC1); a second occurrence would raise confidence.
---
<!-- COMMENTS:END -->
