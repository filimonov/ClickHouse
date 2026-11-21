---
id: CAS-484
title: >-
  Docs say `cas_unsafe_remount_no_delay` is 'not a data hazard'; a live previous
  incarnation can leave a dangling committed ref
status: To Do
assignee: []
created_date: '2026-11-13 17:27'
updated_date: '2026-11-21 08:26'
labels:
  - 'area:lease'
  - 'area:docs'
  - 'origin:review'
  - 'impact:ux'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: docs
ordinal: 589000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
docs/en/antalya/cas/configuration.md:114 says the unsafe remount knob is 'not a data hazard ... availability, not data'. The lease inventory (tmp/prbundle/wave2/lease-inventory.md, 2026-11-13, code read at a3967637843 of PR https://github.com/Altinity/ClickHouse/pull/2831) finds four writes that a still-alive previous incarnation of the same server uuid can start after a successor claimed without the wait: R1 birth + precommit + promote into a namespace with no catalog row while GC's orphan sweep, seeing a higher epoch, deletes the manifest body: a dangling committed ref (model counterexamples superseded_late_unbounded and cur_rowless_blobloss in CaGcRootLocalPartManifestCore_RESULTS.md); R2 commit of a part cloned by trusted-manifest evidence from a stale resident table after the source was dropped and GC deleted the blobs (outside default timing); R3 namespace files written with HEAD + If-Match blind to content: a lost table-level update; R4 catalog Live to Removing by the old incarnation: table wedged in Removing. Correct the docs sentence; decide whether R1 and R2 (small, no format change, need a model rerun) should be closed in code. Not established: no test read, no TLC run, R2's MergeTree callers not traced.
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
created: 2026-11-21 08:26
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 docs/en/antalya/cas/configuration.md:113 still says 'That is not a data hazard ... The exposure is availability, not data'; the same wording is in the setting description ContentAddressedSettings.cpp:82. Changes: impact:ux; type unset -> docs: first deliverable is correcting the documented claim; R1/R2 code decision follows. Pointers stale: configuration.md:114 (now :113).
---
<!-- COMMENTS:END -->
