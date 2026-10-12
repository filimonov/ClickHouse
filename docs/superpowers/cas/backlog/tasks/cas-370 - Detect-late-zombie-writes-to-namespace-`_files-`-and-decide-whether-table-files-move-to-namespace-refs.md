---
id: CAS-370
title: >-
  Detect late zombie writes to namespace `_files/` and decide whether table
  files move to namespace refs
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:write-path'
  - 'area:fsck'
  - 'complexity:large'
  - 'confidence:plausible'
  - 'risk:medium'
  - 'needs:decision'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPlainObjects.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp
  - tmp/noclock/audit/files.md
priority: medium
type: design
ordinal: 464000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`_files/<name>` keys are scoped by namespace life, so a same-srid zombie and its successor share them; `casPutObject`/`casRemoveObject` are fenced only by lease admission. Harm at the MergeTree level, silent until the successor's next restart: phantom mutation from a client-failed `ALTER` (F1), `tmp_mutation_k` name collisions via the per-process `insert_increment` (F5), `removeCurrent` deleting the successor's version after a pause before `HEAD` (F10), missed dedup (F7, F9). Needs a ≥ 16 s stall after admission. Upstream ordering: `MergeTreeSink::commitPart` writes the dedup log before the part commit, so a plain crash can silently drop a retried insert (F8).

Now: detector (~100–150 lines) — `_files` keys whose LastModified (store clock) is after the successor's takeover and whose ETag it did not write; fsck flags `mutation_N` equal to a part's min block. Later: store table files as namespace refs using `ManifestEntry.inline_bytes` (no new wire fields; ~400–700 lines; one-way pool flag plus import, 26.6.4 readers would not see them). Details: tmp/noclock/audit/files.md. Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 detector and fsck check land
- [ ] #2 decision on files-as-refs recorded
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
