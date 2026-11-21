---
id: CAS-503
title: cas-migrate-pool prints two different marker totals after retried deletes
status: To Do
assignee: []
created_date: '2026-11-19 10:38'
updated_date: '2026-11-21 08:24'
labels:
  - cas
  - migration
  - tooling
  - 'area:tooling'
  - 'complexity:trivial'
  - 'impact:operability'
  - groomed-20261121
  - grooming-20261121-changed
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - 'risk:low'
  - 'confidence:solid'
dependencies: []
priority: low
type: bug
ordinal: 608000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Real GCS, pool `ca_live_20260904_r2`, migration during a host network outage: the tool printed `M9: 7402 marker(s) removed` and then `M9: 7260 marker(s) removed, 0 left because they changed during the removal`; the summary line has `markers=7402 ... markers_removed=7260 markers_changed=0`. The store held 0 markers afterwards, so the work was done; 142 removals are counted in one total and not in the other (probably deletes whose first attempt was unanswered and whose retry found the marker gone). Make the two totals agree (count 'already gone' as removed, or print it as its own number). Evidence: `tmp/intents/cloud/round2/gcs/ca_live_20260904_r2/A3_migrate.log`.
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
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Tools/CasMigratePool.cpp:956 meter prints 'M9: N marker(s) removed' and :984 prints a final 'M9: N marker(s) removed, M left' line; both still exist. Changes: area set to tooling; size unset -> trivial (XS): align one counter or print 'already gone' separately; impact:operability; type unset -> bug: two different totals printed for one step. Size: XS. Would settle it: whether the meter at :956 and the final line at :984 read different counters (:956 counts report.markers_read).
---

author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21 (addendum): added risk:low, confidence:solid.
---
<!-- COMMENTS:END -->
