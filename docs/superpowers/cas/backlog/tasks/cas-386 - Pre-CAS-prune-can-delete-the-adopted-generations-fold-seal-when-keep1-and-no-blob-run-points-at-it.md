---
id: CAS-386
title: >-
  Pre-CAS prune can delete the adopted generation's fold seal when keep=1 and no
  blob run points at it
status: To Do
assignee: []
created_date: '2026-10-13 19:14'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:gc'
  - 'risk:high'
  - 'confidence:solid'
  - 'origin:review'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
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
- [ ] #3 TLA+: a model of generations, keep, referenced_generations from run placement, an all-carry round, the pre-CAS prune and a lost CAS; invariant AdoptedSealReadable; the current rule with keep=1 yields a counterexample, the fix is green (CAS-358 spec §7.7)
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-13: sibling CAS-40 (adopted seal naming a run that prune reclaimed, losing leader's pre-CAS prune vs a winner's newer seal) is the same prune-before-CAS family; this task is the sharper case where the ADOPTED generation's own prefix, fold seal included, is the victim. One fix likely closes both: include state.snap_generation in referenced_generations.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 Gc/CasGc.cpp:1330-1345 referenced_generations holds only key_generation of fold_seal.blob_target_runs and parent_seal_runs; prune_floor = generation - keep (pruneSupersededGenerations) so keep=1 reaches the previously adopted G; state.snap_generation is never inserted explicitly. Changes: impact:availability. Pointers stale: CasGc.cpp:3766, :971-979, :1791 (now ~:1345, :1330-1340). Would settle it: whether the prune loop body skips the adopted generation by another test (loop body past the 'LIVE adopted seal still ...' comment not read).
---
<!-- COMMENTS:END -->
