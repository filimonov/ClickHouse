---
id: CAS-501
title: >-
  GC needs about 35 rounds (70 min) for its first complete cut on a pool with
  14.5k ref logs on a real store
status: To Do
assignee: []
created_date: '2026-11-19 10:38'
updated_date: '2026-11-21 08:23'
labels:
  - cas
  - gc
  - performance
  - cloud
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:plausible'
  - 'impact:cost'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: high
type: enhancement
ordinal: 606000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Real GCS, pool `ca_live_20260902_r1` after `cas-migrate-pool` (43,227 bodies, 14,511 ref logs): every GC round logged `destructive work SUPPRESSED this pass — ... frontier INCOMPLETE (2 of 4 namespace(s) proven; unproven: intake_slice_cut=2)`. `fold_ref_intake` stops at 60 s per round and applies 361-467 ref logs (about 7 per second); rounds took 70-219 s (average 109 s). After 33 rounds / 73 min 13,843 of 14,511 logs were applied and no body had been deleted; 27,211 rows were condemned with confirmed intents. The suppression is correct (an incomplete cut must not delete); the cost is the intake rate. Not measured: whether intake reads ref logs one at a time (the rate suggests serial GETs at about 140 ms each). To do: measure intake concurrency; read ahead ref logs on the GC I/O pool; decide whether the 60 s slice should scale. Evidence: `tmp/intents/cloud/round2/gcs/ca_live_20260902_r1/` (`gc_rounds.tsv`, server logs). Not a regression of the blob-intents change; it decides how long a large migrated pool waits for its first reclaim.
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
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 Gc/CasGc.cpp has GcReadAhead (line 2057) and the intake budget kRefIntakeBudgetMs = 60 s (CasGc.h:441), matching the task; no sign of a ref-log read-ahead for the intake. The measurement is in the task text. Changes: area set to gc; size unset -> medium (M): measure, read-ahead on the I/O pool, decide on the slice; risk set to medium; confidence set to plausible; impact:cost; type unset -> enhancement: faster intake on large migrated pools. Size: M. Links: Related to CAS-491 (cold cursor resolution) and CAS-338 (intake GETs). Would settle it: Intake read concurrency on a real store: serial GETs at about 140 ms each, or already windowed.
---
<!-- COMMENTS:END -->
