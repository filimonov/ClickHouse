---
id: CAS-425
title: >-
  Shared warm read cache for CAS blobs: a separate `GET` endpoint for immutable
  blobs in front of a read-only S3 cache
status: To Do
assignee: []
created_date: '2026-10-28 08:34'
labels:
  - 'area:read-path'
  - 'area:backend'
  - 'complexity:small'
  - 'risk:low'
  - 'touches:settings'
  - 'confidence:plausible'
  - 'needs:spec'
  - 'needs:measurement'
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h
  - src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp
  - 'https://github.com/nginxinc/nginx-s3-gateway'
priority: medium
type: feature
ordinal: 528000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A query on node A warms A's filesystem cache; the same data queried on node B hits a cold cache and goes to S3 again. Goal: a blob warmed by one node is warm for every node, with the smallest possible change in ClickHouse.

**Design**
- One shared read cache between the nodes and the bucket, off-the-shelf: `nginx-s3-gateway` (SigV4 to the bucket, `proxy_cache` on disk, `GET`/`HEAD` only, read-only credentials). Any equivalent HTTP cache works.
- One new disk setting, `blob_read_endpoint` (name open): only `GET` of bodies under `blobs/` goes through it. All control traffic (`refs/`, `gc/`, `sessions/`, mounts, every `HEAD`, `LIST`, conditional `PUT`, `DELETE`) keeps going to the store directly, so the cache is outside the correctness path.
- Fallback: an error or timeout from the cache endpoint retries the same `GET` against the store. This fallback is allowed because it never changes an outcome, only latency.
- No invalidation: a blob key is its content hash and the body is immutable. The cache needs no CAS knowledge.
- The per-node filesystem cache stays as L1.

**Deliberately out of scope**: consistent hashing between several caches, a node-to-node protocol (Cloud-style distributed cache with a Keeper registry and a hash ring), write-through on INSERT/merge, HA of the cache. If one cache instance saturates its network, the next step is several instances selected by blob hash on the ClickHouse side.

**Why not route the whole disk endpoint through a read-write S3 proxy.** Zero ClickHouse change, but conditional writes and GC deletes would then pass through third-party software whose bugs land in the write path.

**Why not a Cloud-style distributed cache.** It needs a new interserver endpoint (a sibling of `DataPartsExchange`), a hash ring with Keeper membership and a client-side buffer layer; the shared-cache goal is met without any of it.

**Measure before building**: the share of blob `GET` bytes that are repeat reads across nodes on the otel.demo stand, and whether the hot working set fits one cache host's disk.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A blob read once through node A is served from the cache endpoint to node B, verified by cache hit counters in an integration test with two nodes and one cache
- [ ] #2 Control traffic never hits the cache endpoint, asserted in the same test by the cache's access log
- [ ] #3 Stopping the cache mid-query degrades to direct store reads with no query failure
- [ ] #4 Repeat-read share measured on the otel.demo stand and recorded in the task before implementation
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
