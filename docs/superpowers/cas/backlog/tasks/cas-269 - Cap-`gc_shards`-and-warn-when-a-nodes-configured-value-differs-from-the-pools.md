---
id: CAS-269
title: >-
  Cap `gc_shards` and warn when a node's configured value differs from the
  pool's
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:gc'
  - 'area:formats'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:settings'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
milestone: m-7
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPoolMeta.cpp
priority: low
type: enhancement
ordinal: 334000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`gc_shards` is checked for `!= 0` at the setting (`CA/ContentAddressedSettings.cpp:62`, `:231-236`), at pool-meta creation
(`CA/Pool/CasPoolMeta.cpp:112-113`) and at decode, never for an upper bound, while it sizes per-round vectors. A typo at pool
creation or a planted `_pool_meta` makes every GC round die in allocation. Fails closed; no corruption.
Pool authority is the design, but `Pool::open`/`openForDecommission` overwrite the configured value with no signal
(`CA/Pool/CasPool.cpp:540`, `:959`), so an operator who edits `<gc_shards>` on an existing pool sees nothing.

Provenance: BACKLOG/formats-and-storage.md [gc-shards-no-upper-bound] and #gc-shards-config-override-silent; verified 2026-09-26 against 8b87aa15d21 (cas-gc-rebuild) and 8d62c314ec1 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The setting and `decodePoolMeta` refuse a `gc_shards` above a documented ceiling
- [ ] #2 Opening a pool whose `gc_shards` differs from the configured value logs one WARNING naming both values
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
First recorded: 2026-08-21 (bab19a3de54, by 'gc-shards-config-override-silent')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 ContentAddressedSettings.cpp:262 and Pool/CasPoolMeta.cpp:156 check only gc_shards == 0, no ceiling; Pool/CasPool.cpp:632 and :678 assign config.gc_shards = meta.gc_shards with no warning. Changes: none. Pointers stale: ContentAddressedSettings.cpp:62/:231-236 (now :67/:262); CasPoolMeta.cpp:112-113 (now :156); CasPool.cpp:540/:959 (now :632/:678/:1150).
---
<!-- COMMENTS:END -->
