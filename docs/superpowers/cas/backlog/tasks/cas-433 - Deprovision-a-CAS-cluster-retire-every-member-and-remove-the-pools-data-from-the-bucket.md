---
id: CAS-433
title: >-
  Deprovision a CAS cluster: retire every member and remove the pool's data from
  the bucket
status: To Do
assignee: []
created_date: '2026-11-05 09:45'
labels:
  - 'area:mounts'
  - 'area:gc'
  - 'area:docs'
  - 'needs:spec'
  - 'needs:decision'
milestone: m-8
dependencies:
  - CAS-149
references:
  - docs/en/antalya/cas/operations/migration.md
  - docs/en/antalya/cas/architecture/storage-layout.md
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.cpp
priority: medium
type: design
ordinal: 536000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Nothing today takes a CAS pool to the end of its life. Every verb retires one unit below the pool: `DROP TABLE` removes a namespace's refs and leaves the blobs to a later GC round, `SYSTEM CAS DROP POOL MEMBER` / `clickhouse-disks cas-drop-member` retires one dead member and again leaves shared blobs to GC, `SYSTEM CAS FORGET` is a node-local assertion with no erasure. Once the last member is gone there is no server to run GC, so a deprovisioned cluster (a deleted `ClickHouseInstallation`, a torn-down stand) leaves `_pool_meta`, `blobs/`, `cas/`, `gc/`, `roots/` and `staging/` under the pool prefix forever (key table: docs/en/antalya/cas/architecture/storage-layout.md#key-table). The operator's only option is a raw prefix delete with their own tooling, with no check that no member still holds a live mount lease and no statement of what is safe to delete in which order.

Owner ask (2026-11-05): a task covering cluster deprovisioning and the removal of the pool's data from CAS.

Points the spec has to settle:
- Order of steps from N live members to an empty prefix: drop tables, stop or decommission members (`DROP POOL MEMBER` refuses a live member, so the last server has to be stopped first and the offline twin run against the bucket), then the pool delete. State what each step leaves behind and which steps are optional when the whole prefix is going away anyway (a full prefix delete makes the per-member drains and the final GC round redundant).
- Whether the final delete is a new verb (`clickhouse-disks cas-drop-pool`, `SYSTEM CAS DROP POOL`) or a documented operator procedure over existing tooling. A verb would delete only under the pool prefix, after proving that no `gc/server-roots/<id>/mount` lease is live and that `_pool_meta` names the pool the operator asked to delete, and refuse otherwise. Compare with the fail-closed path a live member takes today when its pool is deleted out from under it (`vanished` in docs/en/antalya/cas/operations/troubleshooting.md).
- Several pools in one bucket: the delete is prefix-scoped, never bucket-scoped.
- Versioned buckets and soft delete (docs/en/antalya/cas/bucket-requirements.md): a prefix delete leaves noncurrent versions; the procedure has to say what the operator does about them outside ClickHouse.
- Partial deprovisioning: a cluster scaled to zero that will come back keeps its data; the procedure must make the two cases visibly different so nobody deletes a pool that is only paused.

Related: CAS-149 (`UNMOUNT` and disk eject, the per-node half of the teardown), CAS-296.1 (`SYSTEM CAS MOUNT`/`UNMOUNT`), CAS-156/157/158 (member decommission), CAS-321.6 (drop a table's parts in one namespace transaction).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A spec records the deprovisioning sequence and the decision between a pool-delete verb and a documented procedure, with the preconditions that make the final delete safe (no live mount lease, pool identity confirmed, prefix-scoped)
- [ ] #2 The chosen mechanism takes a 2-member pool with data to an empty pool prefix, and refuses while any member holds a live mount lease
- [ ] #3 An integration test provisions a 2-member pool, writes, deprovisions it per the procedure, and asserts zero objects under the pool prefix (including `staging/`, `roots/`, `gc/`) while a second pool in the same bucket is untouched
- [ ] #4 docs/en/antalya/cas/operations/migration.md gets a deprovisioning section: step order, what each step leaves behind, versioned-bucket note, and how a paused cluster differs from a deprovisioned one
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
