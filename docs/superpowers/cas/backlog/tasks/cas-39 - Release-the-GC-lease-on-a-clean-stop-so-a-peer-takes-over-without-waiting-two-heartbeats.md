---
id: CAS-39
title: >-
  Release the GC lease on a clean stop so a peer takes over without waiting two
  heartbeats
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-21 08:17'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:medium'
  - 'touches:protocol'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-8
dependencies: []
references:
  - CA/Gc/CasGcScheduler.h
  - CA/Gc/CasGc.cpp
documentation:
  - docs/superpowers/cas/umbrella-roadmap.md
priority: low
type: enhancement
ordinal: 49000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`GcScheduler::stop` joins the workers and clears the in-process leader hint but leaves the durable `gc/state` lease
untouched (`CA/Gc/CasGcScheduler.h:122-126`). A peer then waits about two heartbeat ticks (~2×`gc_interval_sec`, default
60 s) before stealing. No correctness risk. The roadmap's "Clean shutdown" item asks that a restart cost no lost GC
round. Proposal: a "resign" write mirroring the mount side's farewell. A new protocol step: owner consult first.

Provenance: BACKLOG/gc.md#gc-lease-not-released-on-clean-stop (2031-triage CAS-099); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The owner approves or rejects the resign write
- [ ] #2 If approved, a test shows a peer acquires the lease on its next round after a clean stop, and a crash still needs the observation window
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
First recorded: 2026-08-21 (e811b3d68f4, by 'CAS-099')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:09
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: grep for resign, farewell, releaseLease in Gc/ and ContentAddressedMetadataStorage.cpp finds only the mount-side farewell in Gc/CasOrphanManifestSweep.*; no gc/state resign on scheduler stop. Changes: none. Pointers stale: CA/ prefix means ContentAddressed/; CasGcScheduler.h:122-126 moved (stop comment now near :128).. Would settle it: body of GcScheduler::stop not read: confirm it does not write gc/state. Owner question: Approve a durable resign write to gc/state on clean GC stop (new protocol step, no correctness gain, saves about two heartbeats)?
---
<!-- COMMENTS:END -->
