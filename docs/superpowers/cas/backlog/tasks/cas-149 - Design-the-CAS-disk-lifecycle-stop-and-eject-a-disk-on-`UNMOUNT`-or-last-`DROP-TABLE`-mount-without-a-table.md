---
id: CAS-149
title: >-
  Design the CAS disk lifecycle: stop and eject a disk on `UNMOUNT` or last
  `DROP TABLE`, mount without a table
status: To Do
assignee: []
created_date: '2026-09-26 07:41'
updated_date: '2026-09-26 08:34'
labels:
  - 'area:mounts'
  - 'complexity:epic'
  - 'risk:medium'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'needs:spec'
milestone: m-8
dependencies: []
references:
  - src/Interpreters/Context.cpp
  - src/Disks/DiskSelector.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.cpp
documentation:
  - docs/superpowers/models/CaDiskLifecycle.tla
priority: medium
type: design
ordinal: 191000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A CAS disk, once created, is cached for the life of the process: `Context::getOrCreateDisk`
(`src/Interpreters/Context.cpp:6771-6785`) adds it to the disk map and nothing removes it. `DROP TABLE` leaves the disk,
its mount lease renewer and its GC scheduler running, and a leaked GC thread can abort at shutdown.
The rev.8 round met the per-node goals only through `SYSTEM CAS FORGET`; the Dormant/UNMOUNT/MOUNT reuse machinery was
rolled back (`1dc57a8d820`, spec rev.8 §9).
Owner goals (2026-07-22): `UNMOUNT` stops all background work of the disk (GC, lease renewal, remount, sweepers) and ideally
ejects it from the registry; `MOUNT` can bring up a disk, including an inline `disk(...)`, without creating a table;
separate GC stop and start (done: `SYSTEM CAS GC STOP/START`).
Ejecting from the registry touches generic `DiskSelector`/`Context` code, so the spec needs an upstream-coupling verdict.
A disk removed from config has the same shape (`CAS-67`).

Provenance: BACKLOG/mounts-and-lifecycle.md#disk-lifecycle-rev8-closure (the disk-lifecycle-leak proper); verified 2026-09-26 against b1c34d03479 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A spec states the `UNMOUNT`, `MOUNT` and last-`DROP TABLE` semantics and is reviewed against the invariants in AGENTS.md §2
- [ ] #2 After `UNMOUNT` of a CAS disk, no thread of that disk remains and its mount lease is released, shown by a test
- [ ] #3 A stateless test can create and fully remove a uniquely-named CAS disk without leaving a running GC or renewer in the server
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
