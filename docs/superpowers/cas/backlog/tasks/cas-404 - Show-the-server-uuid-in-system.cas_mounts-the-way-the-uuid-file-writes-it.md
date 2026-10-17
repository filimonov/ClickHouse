---
id: CAS-404
title: Show the server uuid in system.cas_mounts the way the uuid file writes it
status: To Do
assignee: []
created_date: '2026-10-17 03:43'
labels:
  - 'area:mounts'
dependencies: []
priority: high
ordinal: 502000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found by soak S47 on PR #2574: system.cas_mounts.server_uuid shows the uuid with its 64-bit halves swapped. Uuid file 35578f40-9661-41c4-a18d-426eec6f5a16, table a18d426e-ec6f-5a16-3557-8f40966141c4. Cause: UUID(m.lease.server_uuid) in StorageSystemContentAddressedMounts.cpp builds a DB::UUID from the UInt128 without the half order ClickHouse UUIDs use. Pre-existing (not PR C). An operator matching the table to a uuid file (rejoin, decommission, duplicate-id diagnosis) sees a mismatch. Check every other CAS place that renders server_uuid as a UUID (logs, system tables, refusal texts) for the same mistake.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 system.cas_mounts.server_uuid equals the content of the server's uuid file, pinned by a test with an asymmetric uuid
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
