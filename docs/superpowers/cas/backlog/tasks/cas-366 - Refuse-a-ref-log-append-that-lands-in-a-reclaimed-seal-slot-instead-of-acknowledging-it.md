---
id: CAS-366
title: >-
  Refuse a ref-log append that lands in a reclaimed seal slot instead of
  acknowledging it
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:ref-ledger'
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.cpp
  - tmp/noclock/audit/reflog.md
priority: high
type: bug
ordinal: 460000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A zombie append (or `resolveWedgeOnce` retry) pauses between `gate` and send; meanwhile `planRefCleanup` deletes the successor's seal at `(E, T+1)` while `_ckpt` is in epoch `E+1`. The late `If-None-Match` append lands in the empty key, `mergeCommittedThrough` accepts the contribution, `publishCkpt` returns `IdenticalSkip`, and only lease admission (`op.admitted()`) stands between the zombie and an ack of a record no recovery or fold will read (R4/R5). Lost acknowledged write; safe today only by time.

Fix (no format change): in `publishCkpt`, refuse a non-seal contribution whose `committed_through` equals the durable `last_epoch_seal` and report `SuccessorSeal` (~25 lines). `CaRefTableSnapshotLogCore` keeps seals in a ghost set that `GcCleanupLog` never shrinks and has no epochs; extend it. Details: tmp/noclock/audit/reflog.md (R4, R5). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 model with deletable seals and epochs fails without the refusal and holds with it
- [ ] #2 gtest: a late append into a reclaimed seal slot is reported as fenced, never acknowledged
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
