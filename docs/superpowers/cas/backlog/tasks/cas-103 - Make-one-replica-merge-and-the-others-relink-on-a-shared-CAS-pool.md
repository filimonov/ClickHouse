---
id: CAS-103
title: Make one replica merge and the others relink on a shared CAS pool
status: To Do
assignee: []
created_date: '2026-07-13'
updated_date: '2026-11-21 08:21'
labels:
  - 'area:replication'
  - 'complexity:medium'
  - 'risk:medium'
  - 'touches:settings'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:soak'
  - 'impact:cost'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies:
  - CAS-251
references:
  - src/Storages/MergeTree/MergeFromLogEntryTask.cpp
  - src/Storages/MergeTree/ReplicatedMergeTreeMergeStrategyPicker.cpp
priority: medium
type: task
ordinal: 141000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Every replica runs the same merge, spills its own full scratch and uploads bytes the pool already deduplicates: 186 GiB of scratch for one deduplicated 100 GiB blob in the full-scale campaign.
Replicas fetch instead of merging only when `always_fetch_merged_part` or `execute_merges_on_single_replica_time_threshold` applies (`src/Storages/MergeTree/MergeFromLogEntryTask.cpp:89-131`, `shouldMergeOnSingleReplica`); neither is set for CAS disks, and fetch on CAS is already a metadata-only relink.
First check whether those existing settings remove the double spill on CAS; only then consider a CAS default or merge coordination.

Provenance: BACKLOG/performance.md#scale-findings [replicated double-spill] (+ orphaned 2026-08-04 triage confirmation). Question also named in BACKLOG/replication.md#zero-copy-parity-audit. Verified 2026-09-26 against dd0ed2f263a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A two-replica run with `execute_merges_on_single_replica_time_threshold` set shows one merge and one relink fetch for a large merge, with scratch and upload bytes recorded
- [ ] #2 A decision is recorded: recommend the setting, default it for CAS disks, or design coordination
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
First recorded: 2026-07-13 (45a6c8ee2b6, by 'replicated double-spill')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: docs/en/antalya/cas/index.md is the only CAS file naming execute_merges_on_single_replica_time_threshold; no integration test sets it; AC1 measurement not recorded in the task. Changes: impact:cost; type enhancement -> task: measurement then a recorded decision. Would settle it: the two-replica run of AC1.
---
<!-- COMMENTS:END -->
