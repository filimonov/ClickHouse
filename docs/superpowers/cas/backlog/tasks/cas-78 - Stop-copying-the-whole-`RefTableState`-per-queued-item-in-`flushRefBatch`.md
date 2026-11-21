---
id: CAS-78
title: Stop copying the whole `RefTableState` per queued item in `flushRefBatch`
status: To Do
assignee: []
created_date: '2026-08-04'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:ref-ledger'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:soak'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
priority: low
type: enhancement
ordinal: 103000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CasRefLedger::flushRefBatch` copies the full committed ref table per item (`RefTableState item_scratch = working;`, `R/Pool/CasRefLedger.cpp:3130`) and again per multi-op item (`RefTableState shape_check = working;`, `:3170`), O(refs) per item. Both branches.
It was the #1 CPU stack in the 2026-07-16 TXN-Final soak. The otel stand (audit F18/F19) is I/O-bound and shows no CAS CPU hotspot, so this is a scalability smell for insert- and mutation-heavy tables with many refs.
Options: validate against an overlay or undo log, copy-on-write state, or incremental diff.

Provenance: BACKLOG/performance.md#writepath-cost-txn-final [ref-table-copy-commit-path]. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Per-item validation cost no longer scales with the table's ref count (gtest or micro-benchmark with 10k and 100k refs)
- [ ] #2 `CASRefLedger*` and the flush fault-injection tests stay green
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
First recorded: 2026-08-04 (0f266066bef, by 'ref-table-copy-commit-path')

2026-11-17: PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit 18be198c7ab: an append is one `_log` create; `_ckpt` is written only at creation and by snapshot publication. The premise "an append is 4 requests" no longer holds.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: Pool/CasRefLedger.cpp:3699 'RefTableState item_scratch = working;' and :3739 'RefTableState shape_check = working;' still copy per item. Changes: none. Pointers stale: CasRefLedger.cpp:3130 now :3699, :3170 now :3739.
---
<!-- COMMENTS:END -->
