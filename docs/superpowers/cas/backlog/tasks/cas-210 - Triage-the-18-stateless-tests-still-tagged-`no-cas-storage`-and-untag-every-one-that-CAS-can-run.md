---
id: CAS-210
title: >-
  Triage the 18 stateless tests still tagged `no-cas-storage` and untag every
  one that CAS can run
status: To Do
assignee: []
created_date: '2026-09-04'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:testing'
  - 'area:ci'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - tests/queries/0_stateless
documentation:
  - docs/superpowers/cas/2026-09-04-stateless-lane-triage.md
priority: medium
type: task
ordinal: 267000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
18 tests carry `no-cas-storage` at HEAD (tag renamed in `c4f0ba4184f`): broken-part and lost-part tests (`02253`-`02255`, `02369`, `02370`, `02444`, `04215`),
`s3_plain` DROP (`02980` x2), fetch-partition pool (`03350`), export-part (`03572` x3, `03608`), reader-executor (`04316`, `04327`, `04328`) and `04286`.
Each tag hides a behaviour the CA-s3 lane never exercises. Point-fixes landed earlier and the full local run of 2026-09-04 (11137 tests) had no class-B 404.
Classify each as recoverable, real CAS bug, or genuinely unsupported, following the `cas-test-triage` procedure.

Provenance: BACKLOG/testing-and-ci.md [ca-s3-stateless-lane]; tag count re-measured 2026-09-26 against 8b87aa15d21.

Update 2026-11-21: Remaining scope at work/antalya-26.8/cas-blob-intents: 7 tagged tests (02980_s3_plain_DROP_TABLE_MergeTree, 02980_s3_plain_DROP_TABLE_ReplicatedMergeTree, 03350_alter_table_fetch_partition_thread_pool, 04286_cas_remote_data_paths, 04316_reader_executor_basic, 04327_reader_executor_metrics, 04328_reader_executor_kpi_async_metric). Whether the 11 untagged ones are green on the CA-s3 lane was not checked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each of the 18 tests is untagged and green on the CA-s3 lane, or keeps the tag with a one-line reason in the test file
- [ ] #2 Every real CAS bug found gets its own task
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
First recorded: 2026-09-04 (5d9bb3ec707, by 'ca-s3-stateless-lane')

2026-10-22, on `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8): `EXPORT PART`/`EXPORT PARTITION` are allowed on CAS disks (`MergeTreeData::checkAlterPartitionIsPossible`) and the export-partition descriptor write is atomic there (`MergeTreePartitionExportScheduler::persist`); the six export tests (03572 x3, 03608 and two more) lost `no-cas-storage` and pass on the CAS-S3 lane. Recount the tag after the branch lands.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: rewrite (high). Evidence: cas-pr1 tests/queries/0_stateless: only 7 files still carry no-cas-storage: 02980 x2 (s3_plain DROP), 03350, 04286, 04316, 04327, 04328. The broken-part and lost-part tests (02253-02255, 02369, 02370, 02444, 04215) and the export tests (03572 x3, 03608) no longer carry it.. Changes: impact:operability; description: Update paragraph appended. Pointers stale: '18 tests' count is out of date (now 7 files); the list in the description includes 11 already untagged..
---
<!-- COMMENTS:END -->
