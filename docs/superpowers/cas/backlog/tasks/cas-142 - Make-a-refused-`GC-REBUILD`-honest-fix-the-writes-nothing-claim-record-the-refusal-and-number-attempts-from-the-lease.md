---
id: CAS-142
title: >-
  Make a refused `GC REBUILD` honest: fix the 'writes nothing' claim, record the
  refusal, and number attempts from the lease
status: To Do
assignee: []
created_date: '2026-07-13'
updated_date: '2026-11-21 08:26'
labels:
  - 'area:gc'
  - 'area:observability'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-7
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h
priority: low
type: bug
ordinal: 184000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`runGcRebuildNow`'s contract says "a refused rebuild writes nothing"
(`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h:206-208`). The last refusal,
a lost `gc/state` CAS (`Gc/CasGc.cpp:4576-4582`), comes after the flushed runs and the fold seal are already written
(`:4565`), so it leaves a complete durable residue. The residue is never adopted and later pruning removes it.
Attempt numbering comes from the per-shard flush count (`:4561-4563`), not from `lease.seq`.
A performed rebuild emits a `GcRebuild` `cas_log` event (`:4588`) but no `cas_gc_log` row, and a refusal records nothing.

Provenance: BACKLOG/gc.md#rebuild-refusal-leaves-run-and-seal-residue (2031-triage CAS-094) and [gc-rebuild follow-ups] part (a), no gc-round-log row for `rebuildBaseline`. Verified 2026-09-26 against d4be7f7045a and 0dbbd797792.

Update 2026-11-21: The header comment no longer claims a refused rebuild writes nothing (ContentAddressedMetadataStorage.h:220-222). Remaining scope to confirm: an audit record for refused rebuilds (AC #2) and attempt numbering from the lease rather than the per-shard flush count (AC #3).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The header comment matches the code: which refusals write nothing and which leave pruned residue
- [ ] #2 Every rebuild, performed or refused, leaves one audit record with its refusal reason, and a gtest checks the lost-CAS refusal writes it
- [ ] #3 Rebuild attempt numbers cannot collide across two refused rebuilds of one generation, proven by a gtest
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
First recorded: 2026-07-13 (45a6c8ee2b6, by 'gc-rebuild follow-ups')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:26
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: ContentAddressedMetadataStorage.h:220-222 now says a refused rebuild 'installs no baseline' (the 'writes nothing' claim is gone, AC #1 looks done); Gc/CasGc.cpp:5270-5276 still sets rep.refusal for the lost gc/state CAS and the GcRebuild cas_log event is emitted at :5287. Changes: description: Update paragraph appended. Pointers stale: CasGc.cpp:4561-4588 moved (rebuild now ~:5270-5290). Would settle it: whether a refused rebuild now leaves an audit record (AC #2) and whether attempt numbers come from lease.seq (AC #3); neither was traced.
---
<!-- COMMENTS:END -->
