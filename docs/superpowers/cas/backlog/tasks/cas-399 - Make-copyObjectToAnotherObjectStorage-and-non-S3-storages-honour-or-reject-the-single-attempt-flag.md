---
id: CAS-399
title: >-
  Make copyObjectToAnotherObjectStorage and non-S3 storages honour or reject the
  single-attempt flag
status: To Do
assignee: []
created_date: '2026-10-15 11:41'
updated_date: '2026-10-24 12:24'
labels:
  - 'area:backend'
  - 'area:upstream'
  - 'touches:upstream-code'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
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
