---
id: CAS-309
title: >-
  Run S23's 1-server and 10-server idle baselines instead of recording them
  inconclusive
status: To Do
assignee: []
created_date: '2026-09-26 14:38'
updated_date: '2026-11-21 08:21'
labels:
  - 'area:soak'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'needs:measurement'
  - 'origin:soak'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:done'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
references:
  - utils/ca-soak/scenarios/cards/s23_s27_misc.py
  - utils/ca-soak/docker-compose-10replicas.yml
priority: low
type: task
ordinal: 388000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
S23's spec asks for idle shared-pool baselines at 1, 2 and 10 servers. The card still hard-codes the 1-server and 10-server cases as inconclusive because "compose is fixed at 2 servers" (`utils/ca-soak/scenarios/cards/s23_s27_misc.py:64-73`).
That premise is gone: `utils/ca-soak/docker-compose-10replicas.yml` runs ten servers on one pool, and a 1-server run needs only ch1 or a one-node compose.
Without these two points the idle cost per added server (GC rounds, lease renewals, S3 requests per idle minute) is unmeasured.
The 2-server idle-memory verdict is a separate question owned by CAS-97.

Provenance: utils/ca-soak/scenarios/BACKLOG.md#NEXT-TASK-scenario-infra-and-inconclusives (S23 item). The entry's other items are owned elsewhere: S15 by CAS-220.1, S16 by CAS-5, S29 by CAS-220.2, drain cost by CAS-215; S20 and S21 pass at full scale (2026-08-31). Verified 2026-09-26 against 66087be0ffb (cas-gc-rebuild); ca-soak is not on altinity/antalya-26.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 S23 runs its 1-server and 10-server variants and reports idle S3 requests per minute and memory for each
- [ ] #2 `RUN_HISTORY.md` has a pass-or-fail S23 row covering all three server counts
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
Grooming 2026-11-21. Verdict: done (medium). Evidence: utils/ca-soak/scenarios/cards/s23_s27_misc.py:62-80 now loops over 1-, 2- and 10-server variants with cluster_boot.reset_cluster (no 'compose is fixed at 2 servers' text left); RUN_HISTORY.md lines 705 and 722 show S23 pass at dev and ci scale (b92bd24789c9, 2026-11-19).. Changes: impact:operability. Pointers stale: s23_s27_misc.py:64-73 hard-coded-inconclusive premise no longer present.. Would settle it: Not checked that the report carries idle S3 requests per minute and memory for each of the three server counts (AC#1), nor a full-scale row..
---
<!-- COMMENTS:END -->
