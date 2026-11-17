---
id: CAS-469
title: >-
  Fix test_database_glue::test_create_table_engine_backend_mismatch_rejected:
  the test expects the storage-backend message, the server returns the
  disk-setting message
status: To Do
assignee: []
created_date: '2026-11-12 21:36'
updated_date: '2026-11-13 12:56'
labels:
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
dependencies: []
priority: low
ordinal: 574000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Not CAS. test.py:1063 expects 'would be reopened with the catalog's storage backend and become unreadable' (DatabaseDataLake.cpp:1261); the server answers Code 36 'Table setting disk differs from its value in this DataLakeCatalog' from validateCreateTableEngine (DatabaseDataLake.cpp:1214), because the test engine uses SETTINGS disk. Job 131348217448; same on base run 43246617309 job 131296539897; red since PR 2792. PR https://github.com/Altinity/ClickHouse/pull/2831 @d8cbdfae3ec, run 43248330323; triage tmp/prbundle/wave1/ci/triage.md. CAS-450 AC 7 needs a public issue link.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824, job 131775013687 (Integration tests amd_asan_ubsan, targeted): the only failure of the job is test_database_glue/test.py::test_create_table_engine_backend_mismatch_rejected (1 failed in 77 s). Same assertion as on the base branch.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824 (triage tmp/prbundle/wave1/ci2/triage.md): glue also fails in 131775013725, 131829639566, 131829639864, 131829639338, 131829639714 (same assertion, same text).
<!-- SECTION:NOTES:END -->
