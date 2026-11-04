---
id: CAS-388
title: >-
  Member lifecycle: decommission deletes data only, a retired member rejoins
  empty
status: Done
assignee: []
created_date: '2026-10-14 17:03'
updated_date: '2026-11-04 22:39'
labels:
  - 'area:mounts'
dependencies: []
references:
  - docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md
  - 'https://github.com/Altinity/ClickHouse/pull/2566'
  - 'https://github.com/Altinity/ClickHouse/pull/2567'
priority: high
ordinal: 483000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Decommission deletes the member's data and keeps its control objects; a retired member rejoins empty at start. Spec: docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md (r10).

Decisions:
- Keep the control objects (owner, epoch, mount) on decommission.
- Write the tombstone first; confirm or roll back afterwards.
- owner_life distinguishes lives of the same server_root_id.
- Rejoin happens at server start; there is no command.
- New server_root_ids may not contain '/'.
- Legacy 26.6.4 states are documented, not repaired.

PR A: https://github.com/Altinity/ClickHouse/pull/2566 (nested ids refused for new owners, abort deletes S3 staging, AbortIncompleteMultipartUpload doc rule, refusal texts).
PR B: https://github.com/Altinity/ClickHouse/pull/2567 (CAS-362's two commits; decommission keeps owner/epoch/mount, tombstone first with confirm/rollback, owner_life, retirement_verified, claim recheck, fenced drain, epoch staging keys, EpochMintPolicy removal, CAS-271, renewer stop-race fix).
PR C: pending (rejoin at Pool::open).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [x] #2 ASan lane green for touched suites
- [x] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [x] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-04: PR A #2566 merged (af662dc591cf). PR B #2567 at 54feac14267 (fix round for k-morozov's review and the 2026-10-23 audit). PR C is https://github.com/Altinity/ClickHouse/pull/2574; B merged into it locally (734d2bcd885), not pushed. All three PRs now target antalya-26.6.

2026-11-04: PR B #2567 merged as 9b348cf0a4b. Remaining: PR C https://github.com/Altinity/ClickHouse/pull/2574 (needs a rebase onto the merged B).

2026-11-04: PR C https://github.com/Altinity/ClickHouse/pull/2574 merged as 11ab9a4224f. A (af662dc591c), B (9b348cf0a4b) and C are all in antalya-26.6; umbrella closed.
<!-- SECTION:NOTES:END -->
