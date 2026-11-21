---
id: CAS-47
title: >-
  Re-measure GC round duration growth on the ASan CAS stateless lane after
  stages B and C
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:gc'
  - 'area:ci'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:issue'
  - 'theme:ci-stabilization'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-1
dependencies:
  - CAS-29.1
  - CAS-29.10
references:
  - CA/Gc/CasGc.cpp
  - CA/Pool/CasPool.cpp
  - 'https://github.com/Altinity/ClickHouse/pull/2300'
priority: low
type: research
ordinal: 57000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR #2300 CI (2026-09-02, `amd_asan_ubsan cas s3 storage 1/2`): round finish times grew 1.3 to 12 minutes over rounds
117-128 (round 128: candidates=7309, deleted=4591, manifests_deleted=8776); by the end of a two-hour run every round
exceeded ten minutes. The shutdown hang it fed is bounded by `Pool::beginTeardown` (`CA/Pool/CasPool.cpp:1087`,
`c80a00aed71`/`9d7e3931c52`); the growth itself is unchecked. Re-run the same load after B1 and the stage C deadline and
confirm rounds stay flat.

Provenance: BACKLOG/gc.md#gc-round-duration-superlinear-growth (from the deleted random/pr2300-ci-triage-20260902.md item 1); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A fresh trace of the same lane shows round duration per round number, before and after stages B and C
- [ ] #2 Any remaining superlinear term is attributed to a phase and filed, or the item closes
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
First recorded: 2026-09-26 (e35929424c6, by 'gc-round-duration-superlinear-growth')

2026-11-13: RCA on PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824 (report tmp/prbundle/wave1/ci2/sanitizer-rca.md): first data from the CI dump of system.cas_gc_log (release arm_binary cas-s3 lane, job 131765755569). Over 90 minutes: GC round duration 20 s to 460 s; ref keys listed per round 3723 to 111086; namespaces seen 241 to 8126; catalog entries 180 to 4294; dead-life debris 98 to 3949. 8 of the last 14 rounds ended Aborted in fold_reduce after 113 to 117 s with 'list of cas_s3/cas/manifests/: gave up at the policy deadline after 14-19 attempt(s)' (the 120 s request window); no S3 error on the GC thread, only slow requests; LIST latency 6.7 to 17.3 ms. Phase seconds over the lane: fold_reduce 1260, defer_decision 898, fold_ref_intake 706, namespace_cleanup 467, ref_object_cleanup 376, manifest_deletes 304, pre_fold_ref_drain 250. Ref cleanup itself works (87117 of 102294 planned objects deleted), so CAS-447's cause is gone; what grows is the listing work per round, and aborted rounds delete nothing, so the backlog feeds itself. The ASan 1/2 server log shows the opposite on the larger runner: 190 rounds, all successful, plateau at 20 to 35 s. Open: why the manifest LIST exceeds the window on the arm lane.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Prerequisites CAS-29.1 (global ref LIST still in place, CasGc.h:1010) and CAS-29.10 (no cas_gc_round_deadline_sec in src) are not done, so the 'after B and C' re-measure cannot run yet; 2026-11-13 note records the pre-fix data (rounds 20 s to 460 s on the arm cas-s3 lane). Changes: none. Links: depends on CAS-29.1 and CAS-29.10 (already stated). Would settle it: a run after B1 and the stage C deadline; open question in the note: why the manifest LIST exceeds the 120 s window on the arm lane.
---
<!-- COMMENTS:END -->
