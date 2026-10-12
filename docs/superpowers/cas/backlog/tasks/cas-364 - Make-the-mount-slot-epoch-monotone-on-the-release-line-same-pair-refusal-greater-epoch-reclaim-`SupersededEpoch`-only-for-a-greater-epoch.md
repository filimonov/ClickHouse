---
id: CAS-364
title: >-
  Make the mount slot epoch-monotone on the release line: same-pair refusal,
  greater-epoch reclaim, `SupersededEpoch` only for a greater epoch
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:mounts'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies:
  - CAS-178
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2512'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/mount.md
priority: high
type: bug
ordinal: 458000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On the base `claimMount` refreshes a slot holding the same `(uuid, epoch)` (T7/MT2) and reclaims for any different epoch, not only a greater one (T8); `classifyFenceCertificate` treats any epoch inequality as `SupersededEpoch`, so a regressed slot hands out false death certificates to `isCreatorFenceTerminal` and `reconcileStaleCreator`. Ref-log R11/R12: a lower-epoch mount recovers a table its own epoch was already sealed in and wedges with a false `CORRUPTED_DATA`.

`3b4037ae265` (lease series, PR #2512) already refuses the same pair and a newer epoch in the slot; `SupersededEpoch` is unchanged there. Fix: land the claim monotonicity (#2512) and require a greater epoch in `classifyFenceCertificate` (~100 lines total). Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/mount.md (M-T7, M-T8), docs/superpowers/cas/2026-10-12-late-request-audit/objects/reflog.md (R11, R12). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `claimMount` refuses the same pair and any epoch not greater than the slot's
- [ ] #2 `SupersededEpoch` requires a strictly greater epoch (unit test with a regressed slot)
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
