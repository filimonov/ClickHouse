---
id: CAS-399
title: >-
  Make copyObjectToAnotherObjectStorage and non-S3 storages honour or reject the
  single-attempt flag
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:backend'
  - 'area:upstream'
  - 'touches:upstream-code'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: low
ordinal: 497000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`copyObjectToAnotherObjectStorage` ignores the creation flag and the unexpected-write retry override. Non-S3 object storages ignore the flag silently, and the mount check covers only the retry profile. This is a trap for future callers. Honour the flag or fail loudly with `BAD_ARGUMENTS` or `NOT_IMPLEMENTED`.

Source: review of https://github.com/Altinity/ClickHouse/pull/2555 (tmp/pr2555/REPORT.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A caller passing the flag to an unsupported path gets an error, with a test
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
2026-10-24: half landed in https://github.com/Altinity/ClickHouse/pull/2555 (fc72b78e78d, merged 2026-10-17): `S3ObjectStorage::copyObjectToAnotherObjectStorage` applies the unexpected-write retry override and the single-attempt creation client, requires `NativeOnly` like `copyObject`, and rejects a non-S3 destination under the flag with `NOT_IMPLEMENTED`; test in `gtest_s3_object_creation_requests.cpp`. Remaining: non-S3 object storages (`AzureObjectStorage`, local, web, wrappers) still ignore `object_storage_single_attempt_object_creation` in their own `writeObject`/`copyObject`, and the CAS mount check (`checkConditionalWriteSingleAttemptSupport`) still keys on `supportsRetryProfile` only. Reachable today only outside CAS Native mode, which refuses non-S3 at mount.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1: object_storage_single_attempt_object_creation is referenced only in ObjectStorages/S3/S3ObjectStorage.cpp, so non-S3 storages still ignore it, matching the 2026-10-24 note (remaining scope). Changes: impact:operability. Would settle it: fc72b78e78d (the S3 half) is not an ancestor of the checked branch; whether the S3 half is in this code was not verified, and the mount check checkConditionalWriteSingleAttemptSupport was not re-read.
---
<!-- COMMENTS:END -->
