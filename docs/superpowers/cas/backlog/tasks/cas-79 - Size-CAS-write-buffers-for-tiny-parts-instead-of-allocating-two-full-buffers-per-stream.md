---
id: CAS-79
title: >-
  Size CAS write buffers for tiny parts instead of allocating two full buffers
  per stream
status: To Do
assignee: []
created_date: '2026-08-04'
updated_date: '2026-11-21 08:19'
labels:
  - 'area:write-path'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'needs:measurement'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - 'theme:parity'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f18
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f18
priority: high
type: enhancement
ordinal: 104000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CaContentWriteBuffer::CaContentWriteBuffer` (`R/ContentAddressedTransaction.cpp:1854-1880`) allocates its own buffer, then a second `WriteBufferFromFile` spill sink, plus `create_directories` and a random temp path, for every stream.
Audit F18: 77.6 of 100 GiB sampled allocation batches carry a CAS frame, all part writers; median part 10 KB (`system.*`) and 1.5 KB (`claude_otel`). On the 2026-08-31 soak the two write-buffer constructors outranked the next CAS frame 200-fold.
Not a leak and not on the critical path: the write path waits ~31x more than it computes. Removing churn may move wall time by nothing; measure first.
Also unmeasured: per-file overhead beyond the double buffer (finalize closure, captured `owner` `shared_ptr`).

Residual from the write-path allocation audit: the emulated test backend copies header maps by value on the write path (test-only cost, no production impact); fold into the same pass or record the exact site.

Provenance: BACKLOG/performance.md#ca-write-buffer-allocation-concentration + #writepath-cost-txn-final [write-path-alloc-audit]; roadmap §2 'Write buffers for tiny parts'. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A scoped profile states what fraction of a blob write's wall time is buffer construction; under 1% closes the task as concentrated but not costly
- [ ] #2 If acted on: allocation bytes per small-part stream drop, measured on the same workload, with no insert-latency regression
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
First recorded: 2026-08-04 (0f266066bef, by 'write-path-alloc-audit')

2026-11-17: the UPLOAD stage is done, the WRITE stage (this task) is not. Commit ac144e848aa on the branch of https://github.com/Altinity/ClickHouse/pull/2831 (local, not pushed yet) sizes the buffers of a blob publication to the blob: blobTransferBufferSize(bytes) = clamp(bytes, 1, DBMS_DEFAULT_BUFFER_SIZE), used for the write buffer of ObjectStorageBackend::publish (native and emulated) and for the reader of the local staged payload. Before it every upload held two 1 MiB buffers charged to the inserting query, so peak memory was (upload pool size) x 2 MiB: 32 MiB at 16 threads, 128 MiB at 64. Found because raising cas_blob_upload_pool_size to 64 made 03770_min_columns_to_activate_adaptive_write_buffer fail with MEMORY_LIMIT_EXCEEDED at 50 MiB on the cas s3 storage lane (a part with 300+ blobs). Still open here: CaContentWriteBuffer, the per-stream buffer plus the spill sink while the part is being WRITTEN; its only bound is clampCasWriteBufferSize (a 256 MiB ceiling); the adaptive write buffer settings reach it (use_adaptive_buffer_size, adaptive_buffer_initial_size). Not sized by ac144e848aa either: the reader of an S3-staged payload. The measurement of acceptance criterion 1 is still not made.

2026-11-17 (later): ac144e848aa is NOT part of PR 2831; it goes to a follow-up PR. It sits on the local branch work/antalya-26.8/cas-upload-buffers (worktree cas-pr1), on top of 44fc40023c1. Until it merges, 03770_min_columns_to_activate_adaptive_write_buffer is red on the cas s3 storage lane with cas_blob_upload_pool_size = 64.

2026-11-17: PR 2831 merged as 0aa837e942e without the upload-buffer fix. This task stays open until ac144e848aa merges in the follow-up PR promised in https://github.com/Altinity/ClickHouse/pull/2831#issuecomment-6959088982.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:19
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: ContentAddressedTransaction.cpp:2087-2113 local-temp ctor still builds an outer buffer plus a second WriteBufferFromFile spill sink per stream; blobTransferBufferSize exists in Backend/CasBackend.h:101 (upload stage done); acceptance criterion 1 measurement not recorded. Changes: removed non-label '>-'; label 'docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget…' moved to references. Pointers stale: R/ContentAddressedTransaction.cpp:1854-1880 is now :2087-2113.
---
<!-- COMMENTS:END -->
