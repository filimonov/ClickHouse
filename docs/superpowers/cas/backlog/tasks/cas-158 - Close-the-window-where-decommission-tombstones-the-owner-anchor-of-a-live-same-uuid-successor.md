---
id: CAS-158
title: >-
  Close the window where decommission tombstones the owner anchor of a live
  same-uuid successor
status: Done
assignee: []
created_date: '2026-08-04'
updated_date: '2026-11-04 15:00'
labels:
  - 'area:mounts'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
  - 'needs:repro'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.cpp
priority: low
type: bug
ordinal: 204000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Between decommission's liveness recheck (the `mount`/`epoch` GETs) and the owner-anchor CAS, a live same-uuid successor can
recreate itself; its owner anchor is then tombstoned, and its next restart is refused with `CORRUPTED_DATA`. Availability,
not data loss: the successor's process keeps running. Accepted explicitly in code ("ACCEPTED RESIDUAL WINDOW",
`CA/Tools/CasDecommission.cpp:419-427`). The older framing "the sweep deletes what the successor needs" was wrong: the
destructive phases run under the victim's own mount lease.
Reproducing it needs a chaos test that restarts the victim between the recheck and the CAS.

Provenance: BACKLOG/mounts-and-lifecycle.md#decommission-toctou-stamps-successor (was [decommission-successor-mount-race], 2026-08-04 triage finding 9); verified 2026-09-26 against b1c34d03479 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A test that restarts the victim inside the window either shows the successor still restartable or fails on the current code
- [x] #2 The accepted-residual comment is removed or narrowed to what remains
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [x] #2 ASan lane green for touched suites
- [x] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [x] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-08-04 (f08734d17df, by 'decommission-successor-mount-race')

2026-10-12 noclock audit (docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md S11): confirmed; harm is a fail-closed wedge on that server's next restart. The epoch-reuse part of the decommission tail is a separate task (never delete `epoch`).

Epoch-reuse part: CAS-363.

2026-10-14: closed by PR B https://github.com/Altinity/ClickHouse/pull/2567: the tombstone is written right after the claim, the claim rechecks owner_life, decommission confirms or rolls back, and a verified mark records retirement. Residual: a tombstone stays over a live successor if the tool dies between its tombstone and the rollback. Availability only; documented and tracked in CAS-385.

2026-10-15: closed by PR B https://github.com/Altinity/ClickHouse/pull/2567 (tombstone after claim, owner_life recheck, confirm/rollback, verified mark); residual 'tool dies before rollback' tracked in CAS-385. Done on merge.

2026-11-04: merged into antalya-26.6 as 9b348cf0a4b (https://github.com/Altinity/ClickHouse/pull/2567). Reds on the final run were base/infra: regression cas_alter_attach_3 (issue #2343), CASS3CacheLightweightDelete, and the cas s3 stateless lanes on `GroupingAggregatedTransform pushed bucket A twice` (not CAS). Delivered by PR B; closed on its merge.
<!-- SECTION:NOTES:END -->
