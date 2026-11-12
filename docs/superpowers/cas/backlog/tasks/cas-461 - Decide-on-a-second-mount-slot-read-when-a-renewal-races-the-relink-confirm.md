---
id: CAS-461
title: Decide on a second mount-slot read when a renewal races the relink confirm
status: To Do
assignee: []
created_date: '2026-11-11 17:33'
updated_date: '2026-11-12 04:59'
labels:
  - 'area:ref-ledger'
  - 'origin:review'
  - 'complexity:small'
  - 'risk:low'
dependencies:
  - CAS-365
priority: low
type: feature
ordinal: 565000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The relink confirm reads the mount slot once after the ledger snapshot and then checks the body against the ownership the mount holds. The read and the check are two steps. A renewal that commits between them changes the slot ETag and ends its attempt, so the check refuses and the confirm answers `Unknown` on a healthy mount. The refusal is safe: it increments `CASRelinkConfirmRefusedSlotMoved` and the receiver falls back as for any `Unknown`. The window is one request round trip per renewal.

Decide from the measured rate whether the confirm should read the slot a second time when the first check fails and the ownership moved in between. A second read changes the modelled fix (`CaRelinkConfirmCore_fix_a`), so the model must be extended before the code changes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The rate of `CASRelinkConfirmRefusedSlotMoved` per confirm is measured on `test_cas_replicated_relink` and `test_cas_gcs_relink_liveness`
- [ ] #2 A decision is recorded: keep one read, or add a re-read with the model extended first
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
Measured 2026-11-12 on `fa1b89d1529` (branch `fix/antalya-26.8/cas-late-request-audit-bundle`): `CASRelinkConfirmRefusedSlotMoved` is 0 of 21 confirms in `test_cas_replicated_relink` and 0 of 160 in `test_cas_gcs_relink_liveness`. `CASRelinkConfirmRefusedSlotUnreadable` is 0 in both. Mount renewals ran at the cadence the modules configure. A load built to make renewals overlap confirms was not run, so acceptance #1 has a first data point and stays open. Report: `docs/superpowers/reports/2026-11-11-cas-pr2-late-request-audit-bundle-measurements.md`, section Relink integration modules.
<!-- SECTION:NOTES:END -->
