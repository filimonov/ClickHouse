---
id: CAS-206
title: >-
  Pin three CAS request classifications by name: write-path unmodeled error,
  `EntityTooLarge`, and `makeCasWriteRetryLaterExceptionPtr`
status: To Do
assignee: []
created_date: '2026-09-04'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:testing'
  - 'area:backend'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - 'impact:wrong-result'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - src/Disks/tests/gtest_cas_requests.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp
priority: low
type: task
ordinal: 263000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Left by the deletion of `gtest_cas_request_control.cpp`. Each shares its path with a tested sibling, so none is a live risk, but none is pinned:
1. no write-path twin of `CASRequests.AnUnmodeledStoreErrorOnAReadIsReissuedNotSurfaced` (`T/gtest_cas_requests.cpp:2018`) shows an unmodeled or `SlowDown` `S3Exception` falling to ambiguity, not `Refused`;
2. `EntityTooLarge` is not tested by name (`isEntityTooLargeError` in the refusal chain at `CA/Backend/CasRequests.cpp:240`; siblings `MalformedXML`/`AccessDenied` are);
3. `makeCasWriteRetryLaterExceptionPtr` (`CA/Backend/CasRequests.h:89`) has no test against `throwCasWriteRetryLater` (`:83`).

Provenance: BACKLOG/testing-and-ci.md#request-control-classification-gaps; verified 2026-09-26 against 8b87aa15d21.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Three new tests next to their siblings in `gtest_cas_requests.cpp`, the third next to `CASWriteResult.OrThrowMapsEveryAlternative`
- [ ] #2 Each test fails when its classification is changed
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
First recorded: 2026-09-04 (229f9e617e8, by 'request-control-classification-gaps')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 src/Disks/tests: makeCasWriteRetryLaterExceptionPtr has no hit in any gtest (item 3 open). EntityTooLarge now appears in gtest_cas_s3_bulk_delete_fallback.cpp:469-486 (a bulk-delete refusal test), so item 2 may be partly met; the request-refusal-chain test in gtest_cas_requests.cpp was not shown to exist.. Changes: impact:wrong-result. Pointers stale: gtest_cas_requests.cpp:2018 and CasRequests.cpp:240 line cites not rechecked.. Would settle it: Whether gtest_cas_requests.cpp pins EntityTooLarge through the CasRequests refusal chain and has the write-path unmodeled-error twin (items 1-2)..
---
<!-- COMMENTS:END -->
