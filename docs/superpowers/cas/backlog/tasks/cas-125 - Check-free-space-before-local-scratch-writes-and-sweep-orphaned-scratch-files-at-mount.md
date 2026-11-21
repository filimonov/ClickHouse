---
id: CAS-125
title: >-
  Check free space before local scratch writes and sweep orphaned scratch files
  at mount
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:21'
labels:
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - 'origin:2031-triage'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp
priority: low
type: enhancement
ordinal: 163000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
No `statvfs`/free-space check exists under `CA/` before a local staging write, and `.tmp` files in `scratch_path`
(`CA/ContentAddressedTransaction.cpp:1010`, `:1865`) left by an unclean restart are never removed; the S3 staging prefix has a
sweeper, local scratch does not. ENOSPC is already fail-loud (audit verdict), so this is about early refusal and leaked disk.

Provenance: BACKLOG/operability-and-introspection.md#disk-error-audit-followups-2026-07-21 (DESIRABLE scratch_path guard); verified 2026-09-26 against dd0ed2f263a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Mount removes scratch files of this server older than the process start
- [ ] #2 A local staging write below a configurable free-space floor fails with a clear error before writing
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
Merged from CAS-101: the full-scale campaign saw local scratch grow 1 -> 21 MiB over an idle window with no user inserts (attribution stays in CAS-101). Scratch files are the local-staging spill of `CaContentWriteBuffer` and the inline-overflow spill in `ContentAddressedTransaction.cpp`; each is removed by its writer on success or failure, so only a crashed process leaves them. A test leaves a stale file and remounts.

First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

2031-triage CAS-046: local scratch is unreserved, unaccounted and never swept at startup; the ledger (#cas-046) maps the `statvfs` guard and orphan sweep to this item. Sizing docs half is CAS-102.

Identifier trace: the earliest docs mention of `ContentAddressedTransaction` is 2026-06-03 (4fb3336fdf3); the finding itself has no record before this migration, so the creation date stays 2026-09-26.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: grep of ContentAddressed/ in cas-pr1: local_scratch_path used only as a path (ContentAddressedMetadataStorage.cpp:292, .h:412,652); no statvfs, free-space check or mount-time scratch sweep. Changes: none. Links: attribution half is CAS-101; docs sizing half is CAS-102; related CAS-264 (S3 staging debris). Pointers stale: CA/ContentAddressedTransaction.cpp:1010,:1865 now ~:1185,:2087+.
---
<!-- COMMENTS:END -->
