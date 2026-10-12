---
id: CAS-374
title: 'Land PR #2516: observers wait the holder''s declared lease, not their own'
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:mounts'
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2516'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp
priority: high
type: bug
ordinal: 468000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Lease length is per server. On the base and on the lease-series tip `3b4037ae265`, `mountObservationThresholdMs` uses the observer's own TTL: holder TTL 180 s, observer TTL 60 s with 10 s cadence fences an unchanged token after 73 s while the holder may still write until 180 s. `CaMountObserveCore` already waits the longer lease, so the model hid the code gap (verified by codex, docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-mount.md). PR #2516 (lease series PR 2 v2) fixes it and is not in the base nor in the 9a–16 chain.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 #2516 merged into the release line
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
