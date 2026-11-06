---
id: CAS-412
title: >-
  Gate fetch-by-relink on a capability cookie instead of replication protocol
  version 11
status: Done
assignee: []
created_date: '2026-10-22 07:59'
updated_date: '2026-11-06 23:12'
labels:
  - 'area:replication'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'touches:upstream-code'
  - 'touches:protocol'
  - 'origin:review'
milestone: m-5
dependencies:
  - CAS-346
references:
  - src/Storages/MergeTree/DataPartsExchange.cpp
priority: medium
type: enhancement
ordinal: 515000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8) `DataPartsExchange` advertises replication protocol version 11 to mean 'this replica can receive a part by relink'. An upstream build that reaches version 11 for its own reasons, or a fork build without the relink confirm step, would be taken for a relink-capable peer (comment in `DataPartsExchange.cpp`: 'still advertises 10 (upstream's current version, or a fork build whose relink had no confirm step)'). Replace the version test with an explicit capability in the fetch request/response (a header or query parameter the CAS build sets and checks), independent of the upstream version counter, so an upstream bump cannot flip the path.

Provenance: owner-Move series PR body follow-up and umbrella review (compat perspective #12), 2026-10-22.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Relink is chosen only when the peer sends the capability; protocol version alone never selects it
- [x] #2 Mixed-version test: a peer advertising version 11 without the capability gets a plain fetch
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
Done on feature/cas-disk-transactions-draft @ 45a60a3f796 (above PR 2647's branch, not pushed): receiver sends client_protocol_version=10 (upstream ceiling) plus cas_capabilities=relink_confirm; the sender offers a relink only on that capability and answers server_protocol_version=min(client,10); constant 11 removed; replication.md gate table updated. Test test_relink_is_selected_by_the_capability_not_the_version: RED on the old binary (v10+capability got no offer), GREEN after; relink module 17/19 (2 failures are 'SYSTEM CAS GC RUN' not parsing in the local container client, unrelated).

2026-11-07 audit: correction: the capability gate (c251a848b8b) merged in https://github.com/Altinity/ClickHouse/pull/2704 as 3ee999a39a6 (antalya-26.8, 2026-11-04), not in PR 2647.
<!-- SECTION:NOTES:END -->
