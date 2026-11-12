---
id: CAS-69
title: >-
  Freeze the retry policy once in `dropNamespaceImpl` and pass it to the
  creator-fence read
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-12 05:00'
labels:
  - 'area:ref-ledger'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h
documentation:
  - docs/superpowers/specs/2026-09-04-cas-hot-key-write-lane-design.md
priority: medium
type: bug
ordinal: 83000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CasRefLedger::dropNamespaceImpl` cancels a stalled `Creating` row through `CasRefCatalog::cancelStalledCreating`, whose callback calls `isCreatorFenceTerminal(cancel_op, layout, ...)` with no policy (`R/Pool/CasRefLedger.cpp:5121`), so the read runs under the default `Retry::standard()` (`R/Pool/CasServerRoot.h:509`).
`resolveNamespaceLife` already freezes once and passes its policy down (`CasRefLedger.cpp:1384`).
Inside a hot-key lane hold that read can keep the `ref_catalog` key for up to its own 90 s past the caller's window.
Fix: freeze once at `dropNamespaceImpl`'s entry and pass the frozen policy to `cancelStalledCreating` and its read. This also makes the hold clamp (`hot-key-hold-clamp`) unnecessary for phase A. Open on both branches.

Provenance: BACKLOG/performance.md#hot-key-lane-phase-b item 0. Verified 2026-09-26 against 59494ebf366 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `dropNamespaceImpl` freezes one policy at entry and every read it issues, including the creator-fence read, uses that policy
- [ ] #2 A gtest with a slow creator-fence read shows the drop gives up at the caller's deadline, not at a fresh 90 s window
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
First recorded: 2026-09-26 (b1c34d03479, by 'hot-key-lane-phase-b item 0')

Implemented 2026-11-12 on branch `fix/antalya-26.8/cas-late-request-audit-bundle` at `fa1b89d1529` (worktree `cas-pr1`, 40 commits over `be74be9fa7b`). Not pushed, pull request not opened. Gates on that head: release `CAS*` 3005, ASan/UBSan 3009, Fast-test compile, stateless 58 of 58. Design: `docs/superpowers/specs/2026-11-11-cas-pr2-late-request-audit-bundle-design.md`. Measurements: `docs/superpowers/reports/2026-11-11-cas-pr2-late-request-audit-bundle-measurements.md`. Story D: `5905e35e911`.
<!-- SECTION:NOTES:END -->
