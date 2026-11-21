---
id: CAS-209
title: >-
  Push the `disk_name` filter down in `system.remote_data_paths` so a query on
  one disk does not walk every disk
status: To Do
assignee: []
created_date: '2026-07-25'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:upstream'
  - 'area:testing'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'origin:review'
  - 'upstream:fixed-master'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-5
dependencies: []
references:
  - src/Storages/System/StorageSystemRemoteDataPaths.cpp
  - tests/queries/0_stateless/04286_cas_remote_data_paths.sh
  - >-
    https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemRemoteDataPaths.cpp
priority: low
type: upstream
ordinal: 266000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`StorageSystemRemoteDataPaths.cpp:153` still has `TODO: applyFilters` on both branches, so every query walks every disk.
This timed out `04286_cas_remote_data_paths` at 600 s; the test is tagged `no-cas-storage` as a mitigation.
Generic code: needs a consult before editing, and belongs in an upstream PR.

Provenance: BACKLOG/testing-and-ci.md#remote-data-paths-no-pushdown; verified 2026-09-26 against 8b87aa15d21 and 8d62c314ec1.

Update 2026-11-21: Upstream master now implements applyFilters for system.remote_data_paths (disk_name pushdown, StorageSystemRemoteDataPaths.cpp:222-260); the PR number was not looked up. This branch still has the TODO at :154, so the work is a sync/backport of that upstream change, not a new upstream PR; the no-cas-storage tag on 04286_cas_remote_data_paths can be revisited after it lands.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `WHERE disk_name = '<d>'` visits only that disk, shown by a test that counts visited disks or by timing on a many-disk config
- [ ] #2 `04286_cas_remote_data_paths` runs on the CA-s3 lane without the `no-cas-storage` tag
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
First recorded: 2026-07-25 (30f0e1dc93c, by 'remote-data-paths-no-pushdown')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-blob-intents src/Storages/System/StorageSystemRemoteDataPaths.cpp:154 still carries the TODO for applyFilters; upstream master already implements ReadFromSystemRemoteDataPaths::applyFilters with a disk_name predicate (StorageSystemRemoteDataPaths.cpp:222-260), so the fix is upstream and the branch lacks it. Changes: upstream reference added; description: Update paragraph appended. Upstream: fixed-master (https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemRemoteDataPaths.cpp); needs a backport.
---
<!-- COMMENTS:END -->
