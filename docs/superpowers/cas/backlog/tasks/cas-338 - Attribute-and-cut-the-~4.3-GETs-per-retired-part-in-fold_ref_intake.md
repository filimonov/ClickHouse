---
id: CAS-338
title: Attribute and cut the ~4.3 GETs per retired part in fold_ref_intake
status: To Do
assignee: []
created_date: '2026-10-01 12:20'
updated_date: '2026-11-21 08:21'
labels:
  - gc
  - performance
  - research
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:plausible'
  - 'impact:cost'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: medium
ordinal: 431000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Measured 2026-10-01 on the CAS-29.9 A/B stand: the round after a DROP of 20k parts issues 86 447 DiskS3GetObject on base and 86 214 on the branch, i.e. about 4.3 GETs per retired part, and fold_ref_intake alone is 287 s at 20 ms RTT (22 s whole round at 0 ms). After CAS-29.9 the janitor is no longer the bottleneck of a DROP; the intake reads are. Research first: split the 86k GETs by family (CASGCGet vs CASOtherGet in the per-round ProfileEvents of tmp/gc-cuts-stand/value/m1-ab-after-20000-lat0-1.json, then by key kind: _log entry, manifest body, ckpt, meta), name which reads the fold needs for owner edges and which are avoidable or batchable (read-ahead through the fold pool as CAS-30 did for .meta markers; manifest decode cache CAS-285). Then propose the cut with an A/B prediction before building anything.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The 86k GETs of the 20k-part DROP round are attributed per key kind with counts, in a report under docs/superpowers/reports/
- [ ] #2 A proposal names which reads are necessary, which are avoidable, the predicted GET count and round time, and the cost of the change
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
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: No report on the 86k-GET attribution found under docs/superpowers/reports (searched fold/intake names); task has no later note saying work started. Changes: area set to gc; size unset -> medium (M): attribution run on the stand plus a proposal; risk set to low; confidence set to plausible; impact:cost. Size: M. Links: Related to CAS-337 (same 20k-part DROP round). Would settle it: Whether the blob-intents GC changed the fold_ref_intake read pattern enough that the 4.3 GETs/part figure is stale.
---
<!-- COMMENTS:END -->
