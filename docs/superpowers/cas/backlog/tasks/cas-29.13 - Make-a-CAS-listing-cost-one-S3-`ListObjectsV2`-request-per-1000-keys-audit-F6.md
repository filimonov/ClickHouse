---
id: CAS-29.13
title: >-
  Make a CAS listing cost one S3 `ListObjectsV2` request per 1000 keys (audit
  F6)
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:18'
labels:
  - 'area:backend'
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:measurement'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-1
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp
  - src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f6
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#verification-items
parent_task_id: CAS-29
priority: high
type: task
ordinal: 174000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On otel.demo `CASRefGlobalListPages` is 1543 per round (1000 keys each) but `S3ListObjects` during `defer_decision` is
14.4k: ~320 keys per request, 3x the requests and latency of the LIST (1530 s per round at 88 ms per request).
Stage B removes the global LIST, but the per-life probes, B2 cleanup listing and the janitor use the same path.
Candidate mechanism, from the code: `listUnder` asks the store for `limit + 1` keys on the first page, which is above
S3's 1000-key cap and so takes two requests, and the store default on resumed pages
(`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp:1104-1108`).
`S3ObjectStorage::iterate` hands `max_keys` to `S3IteratorAsync` (`src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp:484-495`).
That iterator prefetches the next batch, which is wasted when the caller stops at its page end.

Provenance: BACKLOG/gc.md#otel-demo-s3-budget-audit-2026-09-25 (F6, conclusion 7, verification item). CAS-29.1 and CAS-29.6 say to confirm this before reusing the path; this task is that confirmation and fix. Verified 2026-09-26 against d4be7f7045a and 0dbbd797792.

Update 2026-11-21: Code part (AC1, AC3) landed in bd712122b60 and e1be3807259 (merged via 0aa837e942e, ancestors of the checked branch). Remaining scope: AC2 only, a stand measurement of S3ListObjects per list page, plus explaining the 9.3 requests per page seen on otel.demo.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A counting-backend or MinIO test lists a 10k-key prefix through `listUnder` and sees at most 11 `ListObjectsV2` requests
- [ ] #2 On the stand, `S3ListObjects` per `CASRefGlobalListPages` (or per janitor page after stage B) is at most 1.1
- [ ] #3 The emptiness probe's first-page bound and the no-`start_after` backend fallback keep their current guarantees, pinned by the existing tests
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
First recorded: 2026-09-26 (fc7b142789d, by 'audit F6')

2026-09-30: measured and attributed (`docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/06-list-requests-per-page.md`; integration test `test_cas_gc_list_pages` and a trace line in `listUnder` exist on the branch `cas/gc-request-cuts`, not merged). No fix yet.
- RustFS, 20,000 `_log` keys under one ref stream: 67 `ListObjectsV2` requests for 21 logical pages (3.19); ref stream alone 60 / 21 = 2.86.
- A full page costs three requests: its own batch, the next batch for the sentinel key (the store's page is 1000 = `limit`, key 1001 is on the next batch), and the prefetch that `nextBatch` schedules unconditionally (`ObjectStorageIteratorAsync.cpp:71-73`).
- Levers (arithmetic, not measured): no prefetch 60 -> 41; no sentinel lookahead 60 -> 41; both -> 21. The prefetch lever works on any store and does not change the page contract.
- Why it matters: with one GC round per 300 s, `defer_decision` (the listing phase) is 57-58% of the round on local RustFS (`docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/03-long-rounds-and-latency.md`). otel.demo's 9.3 requests per page is not explained by these runs.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commits bd712122b60 "List CAS pages of 999 keys so the sentinel fits the batch" and e1be3807259 "Let a list caller turn off the next-page prefetch" cover: #1 a 10000-key walk costs 11 `ListObjectsV2` requests instead of 27 (tests in `gtest_cas_s3_single_attempt_client.cpp`, `gtest_cas_namespace_janitor.cpp`, `gtest_object_storage_iterator_async.cpp`); #3 the first-page probe bound and the no-`start_after` fallback are pinned by existing tests plus new ones. `ObjectStorageIteratorAsync.{h,cpp}` is the only shared upstream code touched (`prefetch_next_page`, default `true`). Still open: #2 stand ratio of `S3ListObjects` per list page (not measured); the 9.3 requests per page seen on otel.demo remain unexplained. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Delivered in PR 2831 by `bd712122b60` and `e1be3807259`: a 10000-key walk costs 21 requests instead of 27 (and 11 without prefetch). Open: the stand ratio.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:18
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: bd712122b60, e1be3807259, 0aa837e942e are ancestors of the checked branch; task notes say AC1 and AC3 delivered, AC2 (stand ratio) unmeasured. Changes: type bug -> task: fix is delivered; what remains is a measurement; description: Update paragraph appended. Size: S. Links: remaining scope is the AC2 stand measurement only. Would settle it: stand measurement of S3ListObjects per list page (AC2) and the unexplained 9.3 requests per page on otel.demo.
---
<!-- COMMENTS:END -->
