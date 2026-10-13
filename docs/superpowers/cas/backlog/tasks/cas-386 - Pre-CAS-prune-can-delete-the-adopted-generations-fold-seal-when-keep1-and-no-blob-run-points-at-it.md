---
id: CAS-386
title: >-
  Pre-CAS prune can delete the adopted generation's fold seal when keep=1 and no
  blob run points at it
status: To Do
assignee: []
created_date: '2026-10-13 19:14'
labels:
  - 'area:gc'
  - 'risk:high'
  - 'confidence:solid'
  - 'origin:review'
dependencies: []
priority: high
ordinal: 481000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
pruneSupersededGenerations (Gc/CasGc.cpp:3766) runs before the round's gc/state CAS with prune_floor = new_generation - gc_snapshot_generations_to_keep. With keep = 1 the floor is the adopted generation G. referenced_generations (CasGc.cpp:971-979) holds only the generations blob RunRefs physically live in, so a G whose shards were all pure-carry (its blob runs live in H < G) is unreferenced and its whole prefix, fold seal included, is deleted. If that leader then loses its CAS, the adopted seal is gone: the next fold throws at CasGc.cpp:1791 and the pool needs SYSTEM CAS GC REBUILD. Found by the codex review of the CAS-358 spec (tmp/cas358/codex_spec_review_2.md finding 2); independent of the owned-manifest family, which only adds another object to the same prefix. Likely fix: always include the adopted generation (state.snap_generation) in referenced_generations, or never let prune_floor reach it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Failing-first gtest: keep=1, an all-carry round, a pre-CAS prune followed by a lost CAS; the adopted fold seal survives and the next round folds
- [ ] #2 pruneSupersededGenerations never deletes the adopted generation's prefix regardless of RunRef placement
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
