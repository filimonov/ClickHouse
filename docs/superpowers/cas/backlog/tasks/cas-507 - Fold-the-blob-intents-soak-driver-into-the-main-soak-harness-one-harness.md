---
id: CAS-507
title: Fold the blob-intents soak driver into the main soak harness; one harness
status: To Do
assignee: []
created_date: '2026-11-19 22:48'
updated_date: '2026-11-21 08:24'
labels:
  - cas
  - soak
  - tests
  - 'area:soak'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:done'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: chore
ordinal: 612000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Two soak harnesses exist. The main one, `utils/ca-soak/soak/`, has the fault set (`chaos.py`: kill, restart, short `docker pause`, a long freeze of one replica past the mount-lease TTL plus the GC fence margin, store outage). A second one was written for the blob deletion intents work under `tmp/intents/soak/` (`soak.sh <seconds> <binary>`, `driver.py`): a workload of 40 fixed contents re-inserted and dropped, cross-table copies (`ATTACH PARTITION FROM`, `MOVE PARTITION`, clone), an oracle that counts intents against GC rows and treats seven fsck categories as hard, and only kill -9 as a fault. Its 2 h runs said nothing about a deposed but alive GC leader, because a killed process does not wake up. Do: move the workload and the oracle of the lane driver into the main harness as a profile; keep one fault module; delete the scratch harness; make the README say which profile answers which question. Interim state (2026-11-19): the lane driver calls the main harness's fault module for pause / long freeze.
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
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: done (medium). Evidence: master tree utils/ca-soak/README.md lines 10-11 list the `intents` and `combined` profiles; utils/ca-soak/soak/intents.py holds the intent-churn workload and oracle inside the main harness. Changes: area set to soak; size unset -> trivial (XS): delete the scratch harness after confirming nothing needs it; risk set to low; confidence set to solid; impact:operability; type unset -> chore: cleanup of a duplicate harness. Size: XS. Would settle it: The scratch harness tmp/intents/soak (driver.py, faults.py, bin, cfg) still exists, so 'delete the scratch harness' is not done; one fault module is not checked (tmp/intents/soak/faults.py still exis….
---
<!-- COMMENTS:END -->
