---
id: CAS-95.2
title: >-
  Cache table-level namespace file names per namespace life instead of listing
  them
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-09-26 22:56'
labels:
  - 'area:read-path'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - 'origin:issue'
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPlainObjects.cpp
  - 'https://github.com/Altinity/ClickHouse/issues/2439'
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
parent_task_id: CAS-95
priority: high
type: enhancement
ordinal: 132000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`listDirectory` on a table dir (`R/ContentAddressedMetadataStorage.cpp:1801`) merges part names from the in-memory ref table with table-level files (`format_version.txt`, `mutation_*.txt`, `deduplication_logs/...`) that it LISTs via `listNamespaceFiles` (`:1853`, `:1900`).
`MergeTreeData::clearOldTemporaryDirectories` calls it once per table per minute: the constant 155 LISTs per 10 minutes on a 26-table stand (audit F16).
These files have no index; `CasPlainObjects::putNamespaceFile`/`removeNamespaceFile` (`R/Pool/CasPlainObjects.cpp:44,72`) are the only writers, and the namespace life includes `server_root_id`, so only this node writes them.
Fix: one LIST on first use after start, then write-through updates from those two writers.

Provenance: audit #f16 via BACKLOG/performance.md#scale-findings [startup O(refs)]. Verified 2026-09-26 against dd0ed2f263a and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A second `listDirectory` of the same table dir issues no S3 LIST
- [ ] #2 A file added or removed through `putNamespaceFile`/`removeNamespaceFile` is reflected in the next listing without a LIST
- [ ] #3 Steady-state `CASRootList` on an idle node with tables is zero per minute
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
First recorded: 2026-09-26 (27654231df2, by 'scale-findings [startup O')

Issue #2439 proposal 3, not part of this fix: record table-level file names (or the files) in the ref table next to the part refs, so a cold start needs no LIST either. It is an on-S3 format change (decision-4: new format version with a compatibility path), listed by the issue as a design question only.

2026-09-27: closed as not worth its mechanism after eight codex rounds on spec 2026-09-26-cas-directory-probes-no-list-design.md (see its section 0) and two rounds on the design study 2026-09-26-cas-table-files-as-refs-design.md. Root cause: _files/ PUT and DELETE carry no seal and the backend gives no bound on when an accepted request is applied, so any resident copy of the names can name a deleted object after an unclean crash until the next mount. Reopen only together with sealed table-level files (format generation 2).
<!-- SECTION:NOTES:END -->
