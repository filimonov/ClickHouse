---
id: CAS-11
title: Report bytes per object class in fsck and bytes reclaimed per GC round
status: To Do
assignee: []
created_date: '2026-09-26 06:53'
labels:
  - 'area:fsck'
  - 'area:observability'
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:2031-triage'
milestone: m-7
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.cpp
  - src/Interpreters/ContentAddressedGarbageCollectionLog.h
priority: low
type: enhancement
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`FsckReport::physical_bytes` sums only blob bodies (`CA/Tools/CasFsck.cpp:746`, HEAD top-ups `:760`, `:1075`); `.meta`,
manifests, ref logs, snapshots, `_ckpt`, run files, fold seals, GC state and staging keys are counted nowhere. The only
other byte counter is `namespace_janitor_pending_bytes`. Every `cas_gc_log` counter is an object count
(`src/Interpreters/ContentAddressedGarbageCollectionLog.h:38-49`), so "how many bytes did this round reclaim" has no answer.
A bucket-total versus `physical_bytes` gap cannot be attributed to any class. Fsck already walks every plane, so a per-prefix
breakdown is cheap. Incomplete MPU parts and `_probe/` debris are the narrow case in `#mpu-and-probe-debris-unaccounted`.
A per-table reclaim forecast is explicitly out of scope; it needs a sharing-model spec first.

Provenance: BACKLOG/operability-and-introspection.md#byte-accounting-blobs-only-and-preview-size-units first half (2031-triage CAS-123); related #mpu-and-probe-debris-unaccounted (u09-oper-c); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The fsck summary line and SQL row report bytes for each object class the scan walks
- [ ] #2 `system.cas_gc_log` round rows carry the bytes of blobs deleted in that round
- [ ] #3 `operations/monitoring.md` states which classes each byte figure covers
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
