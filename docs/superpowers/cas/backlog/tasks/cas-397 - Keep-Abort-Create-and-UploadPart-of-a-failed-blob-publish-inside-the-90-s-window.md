---
id: CAS-397
title: >-
  Keep Abort, Create and UploadPart of a failed blob publish inside the 90 s
  window
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:write-path'
  - 'area:backend'
  - 'complexity:medium'
  - 'risk:medium'
  - 'origin:review'
  - 'confidence:plausible'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: bug
ordinal: 495000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On the blob failure path `AbortMultipartUpload`, `CreateMultipartUpload` and `UploadPart` keep the SDK client with about 500 retries. Under an S3 brown-out one engine iteration of `ensureBlobPresent` can block for tens of minutes, outside the 90 s window. Bound it or document the residual.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A brown-out test or analysis shows an engine iteration is bounded, or the residual is documented
<!-- AC:END -->

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
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: CAS publish uses a single-attempt client only for some calls (ContentAddressedMetadataStorage.cpp:849 single_attempt_control_plane, CasPartWriteTxn.cpp:335 asSingleAttempt); nothing found that bounds Abort/Create/UploadPart (IO/S3/Client.h:221 abort uses the ordinary client); no brown-out test or note. Changes: confidence set to plausible; impact:availability; type unset -> bug: unbounded block outside the 90 s window under a brown-out; no type was set. Links: related CAS-396. Would settle it: which client and retry budget the blob multipart Abort/Create/UploadPart actually use on the failure path, shown by a brown-out test (AC1).
---
<!-- COMMENTS:END -->
