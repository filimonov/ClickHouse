---
id: CAS-427
title: Make the remaining permanent self-remount refusals terminal
status: To Do
assignee: []
created_date: '2026-11-04 00:09'
labels:
  - 'area:mounts'
  - 'complexity:small'
  - 'risk:low'
  - 'origin:review'
dependencies: []
priority: low
type: bug
ordinal: 530000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A self-remount ends in IdentityLost for a foreign, retired or rewritten owner (PR B https://github.com/Altinity/ClickHouse/pull/2567, bffad2662df). Two refusals that no retry can change still back off and retry every 30 s with lifecycle not_live and no CASIdentityLost: an absent owner over a non-empty subtree (claimOwnerOrThrow 'identity lost over existing data') and a mount slot held by another server_uuid (claimMountAwaitingExpiry returns ForeignOwner). Found by the umbrella review of the #2567 remount fix (master/tmp/pr2567/review, arch and ops reports).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 an absent owner over data and a foreign slot holder end the remount in IdentityLost
- [ ] #2 a test per refusal checks lifecycle and CASIdentityLost
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
