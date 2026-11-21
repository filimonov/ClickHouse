---
id: CAS-202
title: >-
  Delete the test-only `ShardReducer` and `manifestCleanupShard` or make the
  round driver use them
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:gc'
  - 'area:testing'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-4
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
priority: low
type: chore
ordinal: 259000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`ShardReducer` and `manifestCleanupShard` (`CA/Gc/CasGcShardPlan.h:58`, `:62`) have no production caller on either branch.
The live sharded fold buckets by `blobShard` and calls `foldDeltasIntoGeneration` directly (`CA/Gc/CasGc.cpp:3328`, `:3372`, `:4319`);
`CasGc.cpp:3343` names `ShardReducer` only in a comment. Tests of the dead API prove nothing about the running fold.
`manifestCleanupShard` is the only routing function for the part-manifest cleanup axis, so deleting it must be a stated decision.

Provenance: BACKLOG/testing-and-ci.md#shard-reduce-api-production-dead; verified 2026-09-26 against 8b87aa15d21 and 8d62c314ec1.

Update 2026-11-21: ShardReducer is already deleted (75fb01405ab). Remaining scope: decide whether to delete the test-only manifestCleanupShard (Gc/CasGcShardPlan.h:58, .cpp:7) with its tests in gtest_cas_gc_shard_plan.cpp:162+, or wire it into part-manifest cleanup routing; record the decision in the commit message.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Either the API is gone and its tests target `foldDeltasIntoGeneration` and the live bucketing, or the round driver constructs one reducer per shard
- [ ] #2 The decision about the manifest-cleanup routing function is recorded in the commit message
- [ ] #3 `CAS*` gate green
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
First recorded: 2026-08-21 (2583e3427aa, by 'shard-reduce-api-production-dead')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: rewrite (high). Evidence: commit 75fb01405ab 'Remove the CAS GC shard reducer that only tests called' deleted ShardReducer (no match in cas-pr1/src); manifestCleanupShard still defined at Gc/CasGcShardPlan.h:58 and .cpp:7, callers only gtest_cas_gc_shard_plan.cpp and a comment at CasGcShardPlan.h:69. Changes: description: Update paragraph appended. Pointers stale: ShardReducer (CasGcShardPlan.h:58 now holds manifestCleanupShard); CasGc.cpp:3328/3343/3372/4319 line cites not rechecked.
---
<!-- COMMENTS:END -->
