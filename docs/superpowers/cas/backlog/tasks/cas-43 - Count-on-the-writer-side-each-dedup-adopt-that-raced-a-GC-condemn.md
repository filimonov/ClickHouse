---
id: CAS-43
title: Count on the writer side each dedup-adopt that raced a GC condemn
status: To Do
assignee: []
created_date: '2026-07-24'
updated_date: '2026-11-21 08:20'
labels:
  - 'area:observability'
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - 'needs:decision'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-7
dependencies: []
references:
  - CA/Pool/CasPartWriteTxn.cpp
  - CA/Gc/CasBlobInDegree.cpp
  - src/Common/ProfileEvents.cpp
priority: low
type: enhancement
ordinal: 53000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The GC half is done: the recovered-in-degree message is Debug and counted by `CASGCRetiredSparedByReref`
(`5ed8513597d`, `CA/Gc/CasBlobInDegree.cpp:456-467`); ~56 per run, 28 hot hashes in run 30019911967.
The writer still has no signal that it adopted a token GC was condemning, so the race can only be attributed from the
GC side. Add a `BlobAdoptRacedCondemn` ProfileEvent at the adopt site in `PartWriteTxn::ensureBlobPresent`.

Provenance: BACKLOG/gc.md [RECOVERED-INDEGREE-ATTRIBUTION] (residual half; Debug downgrade done in 5ed8513597d); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6.

Update 2026-11-21: The blob-intent rework changed the premise: the writer republishes when it sees a deletion intent (`CASBlobReuseIntentPresent`), so the remaining unobserved case is an adopt where the intent appears only after the writer's read. Decide whether that case needs its own counter before implementing; the `ensureBlobPresent` adopt site named here was reshaped.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A test that forces the adopt/condemn race increments the new writer counter once and the GC spare counter once
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
First recorded: 2026-07-24 (d5be10d4645, by 'RECOVERED-INDEGREE-ATTRIBUTION')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: cas-pr1: BlobAdoptRacedCondemn does not exist; the writer now reads the deletion intent after HEAD and counts the republish case in CASBlobReuseIntentPresent (ProfileEvents.cpp:907, CasPartWriteTxn.cpp:~410-420). CASGCRetiredSparedByReref still exists (CasBlobInDegree.cpp:639).. Changes: label needs:decision; description: Update paragraph appended. Pointers stale: PartWriteTxn::ensureBlobPresent adopt site; CasBlobInDegree.cpp:456-467 (counter increment is now at :639). Would settle it: Whether an adopt that raced a condemn AFTER the intent read (the case the code comment at CasPartWriteTxn.cpp:~408 calls safe but unprovable) is still worth a writer-side counter, now that CASBlobReu…. Owner question: With CASBlobReuseIntentPresent in place, does the adopt-after-intent-read race still need a writer-side counter?
---
<!-- COMMENTS:END -->
