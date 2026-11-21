---
id: CAS-502
title: >-
  cas-fsck does not scale to a 43k-body pool on a real store; one summary run
  sat 8+ min in 'reading gc snapshot runs'
status: To Do
assignee: []
created_date: '2026-11-19 10:38'
updated_date: '2026-11-21 08:25'
labels:
  - cas
  - fsck
  - performance
  - cloud
  - 'area:fsck'
  - 'complexity:medium'
  - 'impact:operability'
  - groomed-20261121
  - grooming-20261121-changed
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - 'risk:low'
  - 'confidence:solid'
dependencies: []
priority: high
type: bug
ordinal: 607000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Real GCS, pool `ca_live_20260902_r1` (43,227 bodies, 43k manifests). (1) `cas-fsck --detail` checks stale edges over one connection, about 0.29 s per manifest: about 3.5 h for this pool (killed after 27 min). (2) `--verify-intents` verifies one intent at a time (about 200 ms each on GCS: 8.8k requests took 14.7 min on a smaller pool). (3) After 33 GC rounds a SUMMARY `cas-fsck --timeout 1800` sat more than 8 minutes on the line `reading gc snapshot runs — 36691 objects, 65536 pages` at 0.9 % CPU and was killed after 12 min 43 s; the same command before the GC rounds finished in 2 min 23 s (845 condemned rows; that phase line never printed). 65,536 pages for 36,691 objects looks like a cap or a power-of-two default; unknown whether it was waiting on the network or idle. To do: reproduce on a pool with tens of thousands of condemned rows (a local store with injected latency is enough), find where it waits, make the three passes concurrent on the tool's I/O pool. Evidence: `tmp/intents/cloud/round2/gcs/ca_live_20260902_r1/A6_fsck_after_gc.log`, `A4_fsck.log`. The pool itself was deleted after the run.
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
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Tools/CasFsck.cpp:629 and :665 read gc state and seal under 'reading gc snapshot runs'; :715 reports progress every 65536 rows, so '65,536 pages' may be that counter, not a cap. Changes: area set to fsck; size unset -> medium (M): reproduce, then make three passes concurrent; impact:operability; type unset -> bug: fsck does not finish on a 43k-body pool. Size: M. Would settle it: reproduction on a pool with tens of thousands of condemned rows to find where the summary run waits.
---

author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21 (addendum): added risk:low, confidence:solid.
---
<!-- COMMENTS:END -->
