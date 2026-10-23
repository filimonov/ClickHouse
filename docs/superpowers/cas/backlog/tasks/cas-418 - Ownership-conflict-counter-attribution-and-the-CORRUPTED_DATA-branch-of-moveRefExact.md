---
id: CAS-418
title: >-
  Ownership-conflict counter attribution and the CORRUPTED_DATA branch of
  moveRefExact
status: To Do
assignee: []
created_date: '2026-10-23 00:28'
labels:
  - cas
dependencies: []
ordinal: 521000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Review of 82ff7d63570..328a1148480 (tmp/review2/REPORT.md in the antalya-26.8 worktree) left two items unverified. (1) `throwCasOwnershipConflict` increments `CASOwnershipConflict` at the throw site; inside an `appendRefOps` builder that may be the ledger leader thread, so the per-query profile may miss it, and a re-run builder counts twice. (2) `moveRefExact` throws `CORRUPTED_DATA` when two rows name one manifest (unreachable under `owned_manifests`); MergeTree may treat `CORRUPTED_DATA` from a rename as a broken part. Decide whether this should be `LOGICAL_ERROR`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Where appendRefOps builders run is documented at throwCasOwnershipConflict; counter attribution matches
- [ ] #2 CORRUPTED_DATA branch either justified in a comment or changed to LOGICAL_ERROR, with the MergeTree rename callers traced
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
