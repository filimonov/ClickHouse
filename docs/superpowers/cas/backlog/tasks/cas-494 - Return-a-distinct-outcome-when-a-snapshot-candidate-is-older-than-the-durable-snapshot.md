---
id: CAS-494
title: >-
  Return a distinct outcome when a snapshot candidate is older than the durable
  snapshot
status: Done
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-17 20:02'
labels: []
dependencies:
  - CAS-487
ordinal: 599000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`publishCkptSnapshot` answers `NotAdvanced` for a candidate older than the durable snapshot. That bumps `CASRefCheckpointNotAdvanced` and starts a publish backoff. Return a separate outcome that does neither. It touches the adoption rule of the snapshot publisher. Rare, because only one publish is in flight. Follow-up of CAS-487.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-15: implemented in the CAS-487 branch (commit 14 of 14): publishCkptSnapshot answers Superseded for a candidate older than the durable snapshot; no CASRefCheckpointNotAdvanced, no backoff, nothing adopted. In production an older candidate cannot arise (one background publish per table); only the explicit publish used by tests can race it. Close this task when CAS-487 merges.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Delivered in PR 2831 by `97123265e03`.
<!-- SECTION:NOTES:END -->
