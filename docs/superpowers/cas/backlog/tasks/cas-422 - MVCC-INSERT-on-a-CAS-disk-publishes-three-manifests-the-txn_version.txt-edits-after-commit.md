---
id: CAS-422
title: >-
  MVCC INSERT on a CAS disk publishes three manifests: the txn_version.txt edits
  after commit
status: To Do
assignee: []
created_date: '2026-10-23 05:42'
labels:
  - cas
dependencies: []
ordinal: 525000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Measured 2026-10-23 on antalya-26.8 (owner-Move series, local object storage): BEGIN TRANSACTION; INSERT ... 1000 rows; COMMIT on a CAS disk costs manifest_put=3, build_publish=2, ref_move=1, ref_repoint=1, build_abort=1. The part itself is one manifest (publish under tmp_insert_* + Move). The two extra manifests come from txn_version.txt being written into the already committed part (storeVersionMetadata / appendCSNToVersionMetadata): two in-place edits of all_1_1_0 within 1 ms, the first staged a manifest and was abandoned (build_abort), the second was published by repoint. Spec 2026-10-07 §9 expected 1 manifest after the commit-order change; it is 3. Oracle: system.cas_log on the test's disk (events above). Not a correctness issue: data and the ref are right; cost only.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 One MVCC INSERT costs one manifest_put on a CAS disk, or the extra edits are shown to be necessary and the number is pinned by a test next to 05396
- [ ] #2 No build_abort for the version-metadata edit
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
