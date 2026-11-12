---
id: CAS-373
title: >-
  Small robustness fixes from the late-request audit: `mergeCkpt` fencing,
  `_pool_meta` vanish, `GetBucketVersioning` bound
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-11-12 05:00'
labels:
  - 'area:ref-ledger'
  - 'area:backend'
  - 'complexity:trivial'
  - 'confidence:solid'
  - 'risk:low'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/snapckpt.md
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/poolprobe.md
priority: low
type: bug
ordinal: 467000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
- SK17 (`Pool/CasRefCkpt.cpp`): `mergeCkpt` throws `CORRUPTED_DATA` for a non-adjacent-epoch contribution without checking `op.admitted()`; return `Fenced` when the writer is not admitted (~10 lines). docs/superpowers/cas/2026-10-12-late-request-audit/objects/snapckpt.md.
- PM-7: `admitOrValidate` throws `LOGICAL_ERROR` when `_pool_meta` vanishes mid-admission (operator erase); use `INVALID_STATE` (~3 lines). docs/superpowers/cas/2026-10-12-late-request-audit/objects/poolprobe.md.
- `GetBucketVersioning` in `checkPoolPreconditions` uses the default client (30 s per attempt, up to 500 retries) and can block mount; use single-attempt or a time window (~10 lines; GCS only).
- Comment in `CasGc.cpp` says an absent checkpoint "WIDENS" cleanup bounds, but `cleanupRefObjects` skips that namespace (prose).
Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 each item fixed with a unit test or marked won't-fix with a reason
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
2026-10-18: draft prepared on fix/antalya-26.6/cas-373-late-request-robustness @0f285d4e01d5 (base 8fefc27bd625, umbrella-reviewed, gated). Local, not pushed; waits for the owner's read. Report, review and body tmp/noclock/drafts/prep/373/.

Implemented 2026-11-12 on branch `fix/antalya-26.8/cas-late-request-audit-bundle` at `fa1b89d1529` (worktree `cas-pr1`, 40 commits over `be74be9fa7b`). Not pushed, pull request not opened. Gates on that head: release `CAS*` 3005, ASan/UBSan 3009, Fast-test compile, stateless 58 of 58. Design: `docs/superpowers/specs/2026-11-11-cas-pr2-late-request-audit-bundle-design.md`. Measurements: `docs/superpowers/reports/2026-11-11-cas-pr2-late-request-audit-bundle-measurements.md`. Story E: `b07b0d9c5bf`, `1cf94f6dbd5`, `0221e08fdf9`.
<!-- SECTION:NOTES:END -->
