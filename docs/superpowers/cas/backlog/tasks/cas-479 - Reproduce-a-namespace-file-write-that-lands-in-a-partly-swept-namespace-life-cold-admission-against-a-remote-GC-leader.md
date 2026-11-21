---
id: CAS-479
title: >-
  Reproduce a namespace-file write that lands in a partly swept namespace life
  (cold admission against a remote GC leader)
status: To Do
assignee: []
created_date: '2026-11-13 04:38'
updated_date: '2026-11-21 08:21'
labels:
  - 'area:ref-ledger'
  - 'origin:review'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:plausible'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: research
ordinal: 584000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
By reading (re-review of the wave 1 final fix, tmp/prbundle/wave1/final/rereview.md, finding N1): when a remote GC leader sweeps a life while this server admits it cold, and the sweep is partial (the _ckpt delete leaked while the page's stream keys were batch-deleted), the checkpoint has no committed_through, recovery takes no catalog cut, the never-born state is not terminal, and a namespace-file write can land in that life. The file is unreachable and a later janitor pass sweeps it; the write linearizes before the DROP, so the effect is a leak, not a wrong read. The gtest ColdWriterCannotWriteIntoALifeSweptByARemoteLeaderDuringItsAdmission covers only a fully swept life. Build the partly swept shape as a gtest, pin what happens, and decide whether the cold path needs a catalog cut before a namespace-file write. Predates wave 1 of PR https://github.com/Altinity/ClickHouse/pull/2831.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-13: related, same re-review (needs verification 2, predates wave 1): a reader of namespace files that hits a resident runtime which cannot recover (its checkpoint was swept) gets CORRUPTED_DATA until eviction, a remount, or a writer's namespaceLife call detaches the runtime; the reader path (namespaceFilesLifeIfReadable) has no catalog check of its own. Decide together with the partly swept case.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: no gtest named for a partly swept life under src/Disks/tests (searched PartlySwept/PartiallySwept and the fully-swept test name: none matched by name). Changes: size unset -> medium (M): gtest for a new shape plus a decision on a catalog cut; risk set to medium; confidence set to plausible; type unset -> research: reproduce, pin, decide. Size: M. Would settle it: the fully swept gtest ColdWriterCannotWriteIntoALifeSweptByARemoteLeaderDuringItsAdmission was not found by name either; check it is still present under another name.
---
<!-- COMMENTS:END -->
