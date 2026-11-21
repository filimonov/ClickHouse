---
id: CAS-5
title: >-
  Emit an event when a writer republishes over a condemned blob, and restore
  S16's direct check
status: To Do
assignee: []
created_date: '2026-08-30'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:observability'
  - 'area:testing'
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:soak'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h
  - utils/ca-soak/scenarios/cards/s15_s18_shards_lifecycle.py
  - src/Disks/tests/gtest_cas_upload_detached.cpp
priority: medium
type: enhancement
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CasEventType::BlobReuseResurrect` is declared (`CA/Primitives/CasEvent.h:19`, `CasEvent.cpp:21`) but has had no emitter
since blob publication became unconditional after a mandatory HEAD (`907c3b5ce7d`). The `Condemned` publication branch
(`CA/Pool/CasPartWriteTxn.cpp:462`) raises no event or counter at all.
The invariant "revival is a fresh re-upload, never a GET of the condemned object" has unit coverage
(`CASUploadDetached.CondemnedLocalResurrection` and `CondemnedS3Resurrection` in `src/Disks/tests/gtest_cas_upload_detached.cpp`
assert a new etag), but the S16 soak card (`utils/ca-soak/scenarios/cards/s15_s18_shards_lifecycle.py:373-388`) lost its
only direct end-to-end check and now relies on a negative proxy that would miss a revival returning correct bytes.
Giving the existing enum member an emitter on the `Condemned` branch fixes the dead vocabulary and the coverage gap together.
Adding an event is not a protocol step: the HEAD-before-PUT sequence is unchanged (decision-1).

Provenance: BACKLOG/operability-and-introspection.md#blob-reuse-resurrect-no-emitter (S16 soak triage 2026-08-30); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6).

Update 2026-11-21: The dead-vocabulary half is done: `BlobReuseResurrect` is emitted at `CA/Pool/CasPartWriteTxn.cpp:418` on the intent-present branch (the `Condemned` branch at :462 no longer exists in that form). Remaining scope: decide whether the row should carry the fresh etag, and give S16 a deterministic condemned-reuse phase that asserts `blob_reuse_resurrect` > 0 (today it asserts adopt + resurrect combined).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A writer that publishes over a condemned blob writes one `cas_log` row with a distinct event type and the new etag
- [ ] #2 S16 asserts that event count is nonzero in its condemned-reuse phase
- [ ] #3 No `CasEventType` member remains without an emitter or an explicit removal
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
First recorded: 2026-08-30 (c9add971c3f, by 'blob-reuse-resurrect-no-emitter')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: Emitter exists: cas-pr1 Pool/CasPartWriteTxn.cpp:418 emits BlobReuseResurrect (outcome intent_present, counter CASBlobReuseIntentPresent), added in 61af4b01913. S16 (utils/ca-soak/scenarios/cards/s15_s18_shards_lifecycle.py:381-391) asserts adopt + resurrect > 0 together and its comment says zero reuse events is legitimate on a fast drain.. Changes: impact:operability; description: Update paragraph appended. Pointers stale: CasPartWriteTxn.cpp:462 Condemned branch; CasEvent.h:19 / CasEvent.cpp:21 line numbers; s15_s18 lines 373-388. Would settle it: AC#1 asks for the NEW etag in the row; the row carries the observed (condemned) etag in `token`, not the republished one. AC#2 (S16 asserts the resurrect count alone is nonzero) is open and conflicts….
---
<!-- COMMENTS:END -->
