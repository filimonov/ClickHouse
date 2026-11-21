---
id: CAS-500
title: >-
  Backport to 26.6: a GC round fails every time over a manifest of a table that
  folded nothing
status: To Do
assignee: []
created_date: '2026-11-18 21:27'
updated_date: '2026-11-21 08:23'
labels:
  - cas
  - gc
  - backport-26.6
  - 'area:gc'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: high
type: task
ordinal: 605000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`manifestDeletionPremise` (`CA/Gc/CasOrphanManifestSweep.cpp`, line 549 on `antalya-26.6`) builds its retain message with `renderRefTxnId(cov.last_folded_ref_id)`. The cursor is legally {0,0} for a live table whose ref log is empty (a table killed between `stageManifest` and its first `precommitAdd`); `renderRefTxnId` throws LOGICAL_ERROR on a zero id; the throw aborts the GC round; the sweep reads the last committed fold seal, so every following round fails the same way. Writes, DROP TABLE and GC REBUILD do not clear it; it ends only when a round's sweep does not reach that manifest (work budget). On a pool with few such tables GC stops pool-wide; debug and sanitizer builds abort. Found by the overlap soak (`tmp/intents/soak/run-20261118-213413`); RCA `tmp/intents/reports/rca-reftxn-zero.md`. Fixed on `work/antalya-26.8/cas-blob-intents` by `fa56fc5e67c` (print `none` for a zero cursor, three tests). The same expression is on `antalya-26.6`: backport the one-line fix and its tests.
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
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Fix fa56fc5e67c is an ancestor of cas-pr1's branch (26.8 side done). The task is the antalya-26.6 backport, which this worktree cannot show. Changes: area set to gc; size unset -> trivial (XS): one-line backport plus tests; risk set to low; confidence set to solid; impact:availability; type unset -> task: backport. Size: XS. Would settle it: Whether the one-line fix and its three tests are on antalya-26.6.
---
<!-- COMMENTS:END -->
