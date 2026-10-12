---
id: CAS-372
title: >-
  GC control plane: deposed-leader false `CORRUPTED_DATA`, `gc/gen` leak below
  the prune cursor, live-pool rebuild
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:low'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - tmp/noclock/audit/gcctl.md
priority: medium
type: bug
ordinal: 466000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The GC control plane is store-safe; three leftovers. S8: a deposed leader whose parent seal the successor pruned throws `CORRUPTED_DATA` ("GC bookkeeping is corrupt … SYSTEM CAS GC REBUILD"), and `REBUILD` (one-off `gc_id`, `allow_steal=false`) refuses while `gc/state` exists, so the operator cannot act. S9: a deposed leader's late artifacts land in a generation already pruned and leak forever (prune cursor only moves forward; fsck does not check `gc/gen`). S2: a round commit the engine gave up on that lands later skips handoff reclaim and manifest deletes (leak until fsck). S17: janitor cursor body has no counter (ETag can repeat; leaks only).

Fix (~60 lines): re-read the lease before S8's throw and report deposed; sweep `gc/gen/<g>/` below `snap_pruned_through` skipping generations the adopted seal references, or an fsck check. Details: tmp/noclock/audit/gcctl.md. Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 deposed leader reports deposed, not corruption (gtest)
- [ ] #2 pruned-generation leftovers are swept or reported by fsck
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
