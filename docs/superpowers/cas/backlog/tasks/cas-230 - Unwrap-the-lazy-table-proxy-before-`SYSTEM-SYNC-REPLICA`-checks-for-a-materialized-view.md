---
id: CAS-230
title: >-
  Unwrap the lazy table proxy before `SYSTEM SYNC REPLICA` checks for a
  materialized view
status: To Do
assignee: []
created_date: '2026-08-22'
updated_date: '2026-11-21 08:24'
labels:
  - 'area:upstream'
  - 'area:replication'
  - 'complexity:trivial'
  - 'risk:low'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'origin:review'
  - 'upstream:unverified'
  - groomed-20261121
  - 'groom-20261121:unclear'
  - 'groom-20261121:certainty-low'
milestone: m-5
dependencies:
  - DRAFT-18
references:
  - src/Interpreters/InterpreterSystemQuery.cpp
priority: low
type: upstream
ordinal: 288000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`trySyncReplica` casts to `StorageMaterializedView` without `unwrapTableProxy` (`src/Interpreters/InterpreterSystemQuery.cpp:2160`) and unwraps
only for the replicated cast (`:2166`), on both branches. With `lazy_load_tables` an unmaterialized MV stays a `StorageTableProxy`, so the MV branch
is skipped and the verb misreports the MV as not replicated instead of syncing its target. Not CAS code; the `lazy_load_tables` class.

Provenance: BACKLOG/testing-and-ci.md#sync-replica-lazy-mv-proxy (opus-review triage T11); verified 2026-09-26 against 8b87aa15d21 and 8d62c314ec1. Siblings: u09-oper-c lazy-proxy-* tasks.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `SYSTEM SYNC REPLICA` on an unmaterialized lazy MV syncs its replicated target table
- [ ] #2 A stateless test covers a lazy database with an MV over a replicated table
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
First recorded: 2026-08-22 (512e55b5bc7, by 'sync-replica-lazy-mv-proxy')

2026-10-22, on `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8): 26.8 replaced `unwrapTableProxy` by `castStorage<T>(table, DeferredTable::Load)` (the fork's SYSTEM-command uses were reverted to it). `trySyncReplica` (`InterpreterSystemQuery.cpp:2317`) still does a raw `dynamic_cast<StorageMaterializedView *>` before the `castStorage<StorageReplicatedMergeTree>` call, so the defect stands; the fix is `castStorage<StorageMaterializedView>(table, DeferredTable::Load)`.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: unclear (low). Evidence: InterpreterSystemQuery.cpp on cas-blob-intents (:2319, :3080) has dynamic_cast<StorageMaterializedView*> but no `unwrapTableProxy` call anywhere in the file, and upstream master's file has none either; the task says an unwrap exists for the replicated cast at :2166. No upstream PR found. Changes: none. Would settle it: whether unwrapTableProxy / StorageTableProxy exists in this branch's tree at all (task may describe the 26.6 lazy_load_tables code only). Upstream: unverified.
---
<!-- COMMENTS:END -->
