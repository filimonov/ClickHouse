---
id: CAS-395
title: >-
  Check what a merge, mutation, REPLACE/ATTACH PARTITION and fetch leave behind
  when their ref-log promote fails
status: To Do
assignee: []
created_date: '2026-10-15 08:43'
labels:
  - 'area:refs'
dependencies: []
priority: medium
ordinal: 493000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The INSERT case is reproduced (a promote-stage _ckpt give-up after a durable PUT, rolled back in memory, replayed by recovery after a restart). CI shows merges, mutations, REPLACE/ATTACH PARTITION and fetch failing at promote too. Their visible effect differs, e.g. a mutation result or a replaced partition reappearing after a restart while the operation reported failure. For each: reproduce with the stall proxy (tmp/dupcheck/rig/stall_proxy.py, .skip counter targets the promote _ckpt GET), state the outcome and whether it is acceptable. Also: ReplicatedMergeTree on CAS (by code the part has no Keeper entry and is detached as unexpected), async_insert=1, the unresolved-PUT (wedge) promote case.
Reproduction and analysis: tmp/dupcheck/REPORT.md (CAS base fef96d50e69).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
