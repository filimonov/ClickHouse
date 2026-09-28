---
id: CAS-321
title: CAS as fast as plain S3 on the same workload (parity umbrella)
status: To Do
assignee: []
created_date: '2026-09-28 16:46'
updated_date: '2026-09-28 16:47'
labels:
  - 'theme:parity'
  - 'area:write-path'
  - 'area:gc'
  - 'area:read-path'
  - 'origin:review'
  - 'confidence:solid'
  - 'complexity:large'
dependencies:
  - CAS-77
  - CAS-85
  - CAS-176.1
  - CAS-176.2
  - CAS-176.3
  - CAS-176.4
  - CAS-167
  - CAS-83
  - CAS-286
  - CAS-287
  - CAS-29.1
  - CAS-29.9
  - CAS-72
  - CAS-13
  - CAS-79
  - CAS-16
  - CAS-25
  - CAS-75
  - CAS-285
  - CAS-223
priority: high
ordinal: 400000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Umbrella for the plan that makes a CAS disk cost no more than a plain S3 disk per operation, and less where CAS can dedup or inline. Yardstick: the same workload on the same store with the same cache, three numbers per operation (wall, S3 requests by kind, bytes), CAS vs plain S3. Baseline 2026-09-28 (tmp/cas223/rig, RustFS rc.3, CI env): create / 3 insert / select / drop cycle = 83 GET + 64 PUT on CAS vs 12 + 31 on plain S3; wall 3.1x (release) after the CAS-223 fixes A+B, 4x before. Root causes and measurements: docs/superpowers/reports/2026-09-28-cas-223-rca/RCA.md. Stages and gates (each stage = one or two reviewable pull requests to antalya-26.6, one mechanism each, numbers before/after in the description): 0 yardstick and CI observability; 1 break the GC feedback loop (drain through the hot-key lane, debris off the round, exact life probes): all rounds succeed under create/drop, catalog bounded, round time flat; 2 foreground cost independent of pool size (CAS-176.1 plus per-namespace catalog lookup): slope of ms/cycle over catalog entries = 0; 3 requests per cycle <= plain S3 (one transaction per part removal, no _ckpt per append, zero-GET publish): GET <= 12, PUT <= 31, wall <= 1.0x; 4 write latency (publish outside DataPartsLock, buffers): wide insert <= plain S3; 5 read path where CAS can win (one-GET part open, inline by size): SELECT/ATTACH GETs <= plain S3, duplicate-content card: fewer bytes and PUTs than S3; 6 consequence: sanitizer CAS shards fit the 6 h budget (CAS-223 closes). Existing tasks are linked as dependencies and labelled theme:parity; the subtasks of this umbrella are the new items only.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Yardstick reports wall, requests by kind and bytes for CAS and plain S3 on identical store and cache, for the cards: create/insert/drop cycle, wide insert, ATTACH 200 parts, mutation, concurrent lightweight delete, column-subset SELECT, duplicate content
- [ ] #2 Stage gates met in order: 1 all GC rounds succeed and round time flat under create/drop; 2 ms/cycle slope over catalog size = 0; 3 GET <= 12 and PUT <= 31 per cycle and wall <= 1.0x plain S3; 4 wide insert <= plain S3; 5 SELECT/ATTACH GETs <= plain S3 and duplicate card below S3 in bytes and PUTs
- [ ] #3 TSan and MSan cas s3 storage stateless shards finish within 1.2x of the s3 storage shards of the same run
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
