---
id: DRAFT-42
title: >-
  Consider recording the store endpoint advisorily in the mount lease to report
  a replicated-prefix mismatch
status: Draft
assignee: []
created_date: '2026-09-26 08:02'
labels:
  - 'area:mounts'
  - 'complexity:small'
  - 'risk:medium'
  - 'touches:on-s3-format'
  - 'confidence:speculative'
  - 'needs:decision'
  - 'origin:2031-triage'
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h
priority: low
type: design
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Pool identity stays `pool_id`-based (endpoint matching is rejected as unsafe, `ContentAddressedExchange.h:157`). An advisory
endpoint in the mount lease would let a mount report that the same pool is being mounted from another endpoint, the signature
of a replicated prefix. The lease body is on-S3 format, frozen since `26.6.4.20001.altinityantalya` (decision-4): any field
needs a new format version and a compatibility path.

Provenance: BACKLOG/docs-and-cleanup.md#pool-exclusive-prefix-undocumented (the 'Optional' sentence); decision-4 applies. Verified 2026-09-26 against 6eb16e1cc56.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A decision records whether the advisory field is worth a format version, given the docs rule of `bucket-requirements-docs`
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
