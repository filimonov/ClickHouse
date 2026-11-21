---
id: CAS-497
title: >-
  GC leaves blob `.meta` markers whose bodies are gone (44 after a 60,008-blob
  drop)
status: To Do
assignee: []
created_date: '2026-11-17 23:48'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:gc'
  - 'origin:benchmark'
  - 'confidence:solid'
  - 'complexity:medium'
  - 'impact:cost'
  - groomed-20261121
  - grooming-20261121-changed
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - 'risk:medium'
dependencies: []
priority: medium
type: bug
ordinal: 602000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Benchmark on the base 0aa837e942e (antalya-26.8 after PR 2831), local RustFS 1.0.6: one INSERT of 340 Wide parts / 60,008 blob bodies, then DROP TABLE SYNC and 80 driven GC rounds (SYSTEM CAS GC RUN). Every body was deleted after 3 rounds (1427 s), but 44 `.meta` objects stayed with no body and no later round removed them. Evidence: master/tmp/intents/bench/BEFORE.md, results/before/rep1/C/. The marker protocol is being replaced by GC-written deletion intents (spec docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md), which removes the marker class and adopts orphan intents; this task decides whether released marker builds (26.6, 26.8 before the switch) need a fix or an fsck rule, and finds which path leaves the marker (candidates: a delete whose marker DELETE lost a race with a restamp; a batch delete partial failure).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The path that leaves a marker without a body is identified from cas_gc_log / blob_storage_log of the benchmark or reproduced in a gtest
- [ ] #2 Decision recorded: fix in the marker builds, or leave to the migration tool's marker removal (step M9) and cas-fsck
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
created: 2026-11-21 08:09
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: `.meta` marker code is still present in the intents worktree (Gc/CasGc.cpp, Gc/CasBlobInDegree.cpp, Formats/CasLayout.h); AC#1 path is not identified in the task. Changes: size unset -> medium (M): investigation plus a decision, maybe a fsck rule; impact:cost; type unset -> bug: orphan markers stay after GC on released marker builds. Size: M. Would settle it: the path that leaves the marker, read from cas_gc_log / blob_storage_log in master/tmp/intents/bench/results/before/rep1/C, and whether the deletion-intents switch has merged.
---

author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21 (addendum): added risk:medium.
---
<!-- COMMENTS:END -->
