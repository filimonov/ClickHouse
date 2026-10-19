---
id: CAS-366
title: >-
  Refuse a ref-log append that lands in a reclaimed seal slot instead of
  acknowledging it
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-10-19 07:02'
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
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/reflog.md
priority: high
type: bug
ordinal: 460000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A zombie append (or `resolveWedgeOnce` retry) pauses between `gate` and send; meanwhile `planRefCleanup` deletes the successor's seal at `(E, T+1)` while `_ckpt` is in epoch `E+1`. The late `If-None-Match` append lands in the empty key, `mergeCommittedThrough` accepts the contribution, `publishCkpt` returns `IdenticalSkip`, and only lease admission (`op.admitted()`) stands between the zombie and an ack of a record no recovery or fold will read (R4/R5). Lost acknowledged write; safe today only by time.

Fix (no format change): in `publishCkpt`, refuse a non-seal contribution whose `committed_through` equals the durable `last_epoch_seal` and report `SuccessorSeal` (~25 lines). `CaRefTableSnapshotLogCore` keeps seals in a ghost set that `GcCleanupLog` never shrinks and has no epochs; extend it. Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/reflog.md (R4, R5). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-13 model (`0db22603534`, CaRefTableSnapshotLogCore RESULTS, new epoch slice): `cur_lateappend` violates `INV_ACKED_REACHABLE` (22 states) when the lease check fails; green with T1. Candidate A (refuse a non-seal contribution whose committed_through equals the durable `last_epoch_seal`) is NOT enough: with a third epoch whose seal is the frontier, `mergeCommittedThrough` accepts the zombie's lower-epoch record by numeric comparison (`fix_a_3ep` red). Recommended A2 in `publishCkpt` `decide`: (1) contribution ct == durable `last_epoch_seal` → `SuccessorSeal` (conclusive); (2) contribution epoch below the durable ct epoch → accept only if the durable seal is in the contribution's epoch and strictly above it, else new outcome `Superseded` (not acked, not rejected; handled like `FencedOut`). Green with 3 epochs (358 k states) and keeps adopted stragglers acknowledged. Optional B3: GC keeps the newest object of the epoch just below the frontier (one object per table). No format change, no extra requests; ~90 lines (`CasRefCkpt.{h,cpp}`, `commitRefChunk`, `resolveWedgeOnce`, recovery sites). Failing-first: `CASRefCkpt.PublishRefusesNonSealAtDurableSeal`, `CASRefCkpt.PublishRefusesLowerEpochPastLaterSeal`, a ledger late-append test (needs a pause point between gate and send). Supersedes the description's candidate A.

2026-10-18: draft prepared on fix/antalya-26.6/cas-366-superseded-ckpt-contribution @2d2182b3b19c (base 8fefc27bd625, umbrella-reviewed, gated). Local, not pushed; waits for the owner's read. Report, review and body tmp/noclock/drafts/prep/366/.
<!-- SECTION:NOTES:END -->
