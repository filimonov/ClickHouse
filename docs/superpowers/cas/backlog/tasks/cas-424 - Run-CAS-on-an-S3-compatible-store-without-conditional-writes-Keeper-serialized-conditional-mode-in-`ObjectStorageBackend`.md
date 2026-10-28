---
id: CAS-424
title: >-
  Run CAS on an S3-compatible store without conditional writes:
  Keeper-serialized conditional mode in `ObjectStorageBackend`
status: To Do
assignee: []
created_date: '2026-10-28 08:34'
labels:
  - 'area:backend'
  - 'complexity:medium'
  - 'risk:high'
  - 'touches:protocol'
  - 'touches:settings'
  - 'confidence:plausible'
  - 'needs:spec'
  - 'needs:decision'
milestone: m-6
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h
  - >-
    docs/superpowers/cas/backlog/decisions/decision-7 -
    GC-blob-deletes-move-to-per-object-conditional-bulk-deletes-S3-DeleteObjects-with-ETag-GCS-batch-ifGenerationMatch-Azure-Blob-Batch-If-Match.md
priority: medium
type: feature
ordinal: 527000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A store that does not enforce `If-None-Match`/`If-Match` cannot host a CAS pool today: `Cas::Probe` refuses the mount at open (fail-closed). Add a third `ObjectStorageBackend::Mode` next to `Native` and `EmulatedSingleProcess` that serializes every conditional write and exact-token delete through a Keeper lock, so such stores become usable without a proxy and without touching the protocol's step sequence.

**Mechanism**
- Conditional `write`/`remove`: take a per-key ephemeral lock in Keeper, `HEAD`, compare the ETag with `expected_value`, `PUT`/`DELETE`, release. The incarnation value stays the store's ETag, so the value-implies-content invariant and `supportsListTokens` are unchanged.
- Fence: a Keeper session loss between lock and `PUT` reports the write as `Unresolved`; the request contract already settles that by reading the key back. Keeper loss is write unavailability for the pool, never data loss.
- Replacement for "only the proxy holds the bucket keys": `_pool_meta` records the Keeper ensemble identity at pool creation; a mount through a different ensemble is refused. Two clusters with separate Keepers on one bucket would otherwise defeat the lock.
- `Cas::Probe` in this mode cannot prove store-side enforcement (there is none); it verifies the lock path instead and logs that the pool runs in emulated mode.
- Bulk conditional deletes (decision-7) have no store-side precondition in this mode: either per-key locks around a keys-only batch, or the single-key exact-token path. Pick in the spec.

**Why not a caching S3 proxy that emulates conditionals.** Only ClickHouse nodes access the bucket, so the proxy's two advantages (S3-API transparency for other consumers, credentials as the fence) are not needed; the proxy adds an HA service in the write path. The `_pool_meta` Keeper binding gives the fence at zero infrastructure.

**Open before the spec**
- Which stores are in scope (which MinIO/Ceph RGW versions, others). Decides whether the task is worth doing at all.
- Whether the `Native` capability probe can still run its op battery on a weak store to detect partial support (for example `If-None-Match` honoured, `If-Match` ignored).

Protocol-adjacent: the step sequence is unchanged, but the primitive's enforcement moves from the store to Keeper, so the spec goes through adversarial review (decision-1 scope).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A pool on a store that fails the native conditional probe mounts writable in the Keeper-serialized mode and passes the CAS gtest gate plus the shared-pool integration test
- [ ] #2 A mount through a Keeper ensemble other than the one recorded in `_pool_meta` is refused
- [ ] #3 A Keeper session lost between lock and PUT surfaces as `Unresolved` and is settled by the existing read-back, with a test
- [ ] #4 The probe logs the emulated mode at mount and refuses the mode on a store that passes the native probe unless explicitly configured
- [ ] #5 Spec reviewed adversarially before implementation
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
