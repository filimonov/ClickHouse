---
id: CAS-358
title: >-
  Stop the orphan sweep from replaying a namespace without a `_snap` from
  genesis on every page
status: To Do
assignee: []
created_date: '2026-10-12 09:50'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:decision'
  - 'needs:measurement'
  - 'origin:review'
milestone: m-1
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2542'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
priority: medium
type: research
ordinal: 452000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The orphan sweep's protection view (`activeManifestKeys` → `recoverRefTableDetailedFromAuthority`) replays a namespace from its `_snap`, or from genesis when it has none, on every page that nominates a candidate in that namespace. A table below the snapshot threshold never publishes a `_snap`, and every remount adds a seal-only epoch, so the replay grows with age and remounts. Production numbers from https://github.com/Altinity/ClickHouse/pull/2542 (one writer, two readers, ~800 tables, 26.6.4 Antalya): 792 streams, 568 with ≥ 2 epochs, 74 with a `_snap`, 3,876 epochs that are a single `epoch_seal`; the replay explains most of the GC leader's ~956 k successful GETs per day after the PR removed the 404 class.

Candidate directions named in the PR, not decided: a cheaper rebuild source (the fold seal's `last_folded_ref_id` plus a cached view), or letting a seal-only epoch count toward the snapshot threshold so idle tables get a `_snap`. The contributor asked which direction we prefer before writing it. CAS-87 (sweep walks the committed tail twice) halves the same reads from the other side.

Research first: attribute the 956 k reads per day by key kind and by namespace age, predict the cut of each option, then pick.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The GC leader's successful GETs per day are attributed by key kind and namespace age, in a report under docs/superpowers/reports/
- [ ] #2 Each option has a predicted cut and cost; one is chosen and the decision is written to the task
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
