---
id: CAS-254
title: >-
  Make a MOVE out of a CAS disk copy file bytes instead of blob envelopes and
  empty inline keys
status: To Do
assignee: []
created_date: '2026-09-26 08:14'
labels:
  - 'area:read-path'
  - 'complexity:medium'
  - 'risk:medium'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'origin:2031-triage'
milestone: m-0
dependencies: []
references:
  - src/Disks/DiskType.cpp
  - src/Disks/DiskObjectStorage/DiskObjectStorage.cpp
  - src/Storages/MergeTree/DataPartStorageOnDiskBase.cpp
  - 'https://github.com/Altinity/ClickHouse/pull/2415'
priority: medium
type: bug
ordinal: 319000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`DataSourceDescription::operator==` ignores `metadata_type` (`src/Disks/DiskType.cpp:35-38`), so a CAS s3 disk and a plain
s3 disk on the same endpoint compare equal and `DiskObjectStorage::copyFile` (`src/Disks/DiskObjectStorage/DiskObjectStorage.cpp:291-322`)
takes the raw server-side copy. `clonePart` branches only on the destination (`src/Storages/MergeTree/DataPartStorageOnDiskBase.cpp:790`),
so `MOVE PART`/`PARTITION` or a TTL move from CAS to such a disk copies `[envelope][payload]` for blobs and an empty key for
inline files. Every real part has inline files, so today the move fails loudly, leaving garbage objects.
Moving data back off CAS is part of the first-deployment gate (roadmap §1, CAS-116).
Open PR #2415 fixes the same `copyFileImpl` path for `BACKUP` (inline bytes via `readInlineDataToString`, blob payload offset
via `getObjectPayloadOffset`); it probably covers this path too, but no test proves it.
Alternatives: refuse the move on a CA source, or make the equality see `metadata_type` (generic code, needs the upstream-consult step).

Provenance: BACKLOG/formats-and-storage.md#move-out-copies-envelope-bytes ([mixed-ca-tiered-topology]); verified 2026-09-26 against 8b87aa15d21 (cas-gc-rebuild) and 8d62c314ec1 (altinity/antalya-26.6). Code-reading finding, so capped at Medium by the priority rule; raise it if the CAS-116 migration run hits it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 An integration test moves a partition from a CAS disk to a plain s3 disk on the same endpoint and back, and the row checksums match
- [ ] #2 The same test covers a TTL move out of CAS
- [ ] #3 If PR #2415 is the fix, the test lands with it or right after it and fails without it
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
