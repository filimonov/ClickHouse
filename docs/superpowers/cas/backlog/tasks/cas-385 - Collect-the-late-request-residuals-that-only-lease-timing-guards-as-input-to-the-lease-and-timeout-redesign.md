---
id: CAS-385
title: >-
  Collect the late-request residuals that only lease timing guards, as input to
  the lease and timeout redesign
status: To Do
assignee: []
created_date: '2026-10-13 10:25'
updated_date: '2026-10-14 17:03'
labels:
  - 'origin:noclock-audit'
dependencies: []
priority: medium
ordinal: 479000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Inputs for the paused mount-lease series (docs/superpowers/cas/2026-10-12-mount-lease-series-state/README.md) and CAS-343. Each entry is a race that no store precondition closes; only the admission fence (local lease timing) stops a paused process. Decide per entry: accept and document, or close by design.

- Decommission admin paused before its LIST of roots/<srid>/: meanwhile the decommission is rerun to a clean farewell, the srid is re-added, and the new member writes roots objects. The old admin resumes, lists and deletes them. roots/ keys carry no epoch, so the store cannot tell lives apart. Source: review of docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md (tmp/noclock/lifecycle/review-codex.md finding 2, review-opus.md finding 3). Owner decision 2026-10-13: document and track here.
- A late drain attempt that hits identical roots/<srid>/ bytes after a rejoin.
- A tombstone left over a live successor if the decommission tool dies before its rollback (availability only).
- A blind rerun after a killed tool and a rejoin drains the rejoined member. Operator rule: check system.cas_mounts first.
- Old-binary races during a rolling upgrade.
- A 26.6.4 starter paused across a decommission and racing rejoins (mixed-version only).
- The remaining late-request classes listed in docs/superpowers/cas/2026-10-12-late-request-audit/README.md.
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
2026-10-14: removed the zombie create/mint bullet (counter or slot): obsolete, because the slot is never absent after PR B https://github.com/Altinity/ClickHouse/pull/2567. Added the five entries from the member-lifecycle spec (late drain after rejoin, tombstone over a live successor, blind rerun, old-binary rolling-upgrade races, 26.6.4 starter across decommission and rejoins).
<!-- SECTION:NOTES:END -->
