---
id: CAS-131
title: Move `CasInMemoryBackend` out of the production `dbms` target
status: To Do
assignee: []
created_date: '2026-08-22'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:tooling'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-4
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.cpp
  - src/CMakeLists.txt
priority: low
type: chore
ordinal: 170000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CA/Backend/CasInMemoryBackend.{h,cpp}` (228 + 490 lines) is linked into `dbms` by the directory glob at `src/CMakeLists.txt:138`,
with no production construction site and no registry entry; production files mention it only in comments
(`CasBackend.h:205`, `CasGcShardPlan.h:81`, `CasPlainObjects.cpp:67`). Its users are gtests
(`gtest_cas_backend.cpp`, `cas_test_helpers.h`, `gtest_ca_wiring.cpp`). Dead weight and an audit smell, no behaviour.

Provenance: BACKLOG/operability-and-introspection.md#in-memory-backend-ships-unused (opus review M10); verified 2026-09-26 against dd0ed2f263a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The backend builds only into the unit-test target, and the `CAS*` gtest gate passes
- [ ] #2 No symbol of `InMemoryBackend` is present in the `clickhouse` binary
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
First recorded: 2026-08-22 (8b9cd77b94e, by 'in-memory-backend-ships-unused')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Backend/CasInMemoryBackend.{h,cpp} still sit under ContentAddressed/Backend/ and src/CMakeLists.txt uses add_headers_and_sources(dbms ...) directory globs for Disks/DiskObjectStorage/MetadataStorages/*. Changes: none. Pointers stale: src/CMakeLists.txt:138 glob line not matched exactly; CA subtree is globbed from a parent entry. Would settle it: the exact add_headers_and_sources line that globs ContentAddressed/Backend into dbms.
---
<!-- COMMENTS:END -->
