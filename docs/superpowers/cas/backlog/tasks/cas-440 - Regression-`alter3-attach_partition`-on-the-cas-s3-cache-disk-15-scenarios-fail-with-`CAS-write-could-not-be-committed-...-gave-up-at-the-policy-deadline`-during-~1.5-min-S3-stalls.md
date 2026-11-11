---
id: CAS-440
title: >-
  Regression `alter3/attach_partition` on the cas-s3-cache disk: 15 scenarios
  fail with `CAS write could not be committed ... gave up at the policy
  deadline` during ~1.5 min S3 stalls
status: To Do
assignee: []
created_date: '2026-11-06 22:01'
updated_date: '2026-11-11 08:00'
labels:
  - cas
  - ci
  - regression
dependencies: []
priority: medium
type: bug
ordinal: 543000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2577 @74562b9f124, job `RegressionTestsRelease / CASS3CacheAlter (attach, 3)`: 15 of 3427 scenarios failed over 2 h 23 m, each after 1 m 22 s - 1 m 48 s of waiting, with `Code: 210 ... CAS write could not be committed (read of 'data/cas/ref_catalog': gave up at the policy deadline after 13 attempt(s)); retrying later`, `CAS _ckpt for namespace ... gave up at the policy deadline after zero attempt(s)` and `CAS ref-log txn 1-9 ... is durable but was not acknowledged (publishing its checkpoint frontier was not confirmed ...)`; one mutation failed with the same cause (Code 341). The same job was green on the previous run of the PR (02a2511744c) and on PR 2805 and PR 2812 the same day; the PR changes only error texts. Report: https://altinity-build-artifacts.s3.amazonaws.com/REFs/2577/merge/74562b9f1244f2b7b585c255106316f4ddabb2d6/regression/x86_64/with_analyzer/zookeeper/without_thread_fuzzer/alter3/attach_partition/cas-s3-cache/nice-new-fails.log.txt
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Find what stalls the object store of the regression environment for about 90 s (MinIO logs, host metrics) or name the CAS request that burns the whole policy budget
- [ ] #2 Record how often the job fails this way on the 26.6 and 26.8 lanes
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
2026-11-08: also on the plain 'cas' disk, not only cas-s3-cache. PR 2805 @ad04f95e441: alter3/attach_partition/cas 2 scenarios, 'gave up at the policy deadline' x10 (pattern #30, CREATE TABLE took 1m20s). PR 2806 @300a8c155c7: alter3/attach_partition/cas 9 scenarios, 'could not be committed' x13. cas-s3-cache was green in both runs. Reports: REFs/<pr>/merge/<sha>/regression/x86_64/with_analyzer/zookeeper/without_thread_fuzzer/alter3/attach_partition/cas/nice-new-fails.log.txt

2026-11-08 root cause (three runs: 2805 alter3/cas, 2806 alter3/cas, 2577 alter3/cas-s3-cache; RustFS stdout in _service_logs/minio.log, node clickhouse-server.err.log, raw.log): every 'gave up at the policy deadline after 13 attempt(s)' on 'data/cas/ref_catalog' sits inside a window where RustFS 1.0.3 rejects GETs with 'GetObject rejected: disk-read hard concurrency cap reached' -> HTTP 503 SlowDown. RustFS admission: RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS (default 64) + degraded lane (default = primary) = 128 permits, a permit is held for the whole body transfer, 5000 ms wait then reject. 2805: 01:39:36-01:40:52 node time, 888 rejections, 387 AWSClient 503 lines; 2806: two windows 10:13-10:14 and 10:22-10:24 UTC; 2577 cache lane: 01:13-01:14. The CAS read policy (Retry::standard, 90 s; attempt envelope 5 s + 2x1 s connect = 7 s -> 13 attempts) is shorter than the 76 s saturation plus the 5 s per-request wait, so the 13th attempt lands before RustFS recovers. Load is not exceptional: ~112k partial-range GET streams per run; the server opens to-EOF ranges, reads ~1 MiB and drops them, which keeps permits busy. The cache lane issues ~40x fewer GETs, hence green more often. Regression env: RUSTFS_SCANNER_ENABLED=false, heal off, MAX_BLOCKING_THREADS=64, DURABILITY_MODE=relaxed. Open: one GC 'zero attempt(s)' event at 02:00:08 (completedRemovingDelete, CasRefCatalog.cpp) with no RustFS trouble, and 4 'NEEDS RECOVERY at committed-frontier publication' lines. Candidate fixes: raise RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS in the regression compose, or bound CAS reads to the byte range they need so permits are released sooner.

2026-11-08 (2805 node err.log, 246 MB): 25 of the 27 'CAS write could not be committed' lines and all 4 'NEEDS RECOVERY at committed-frontier publication' lines (01:40:45-01:40:56) fall inside the RustFS 503 window 01:39:36-01:40:52. The other 2 (02:00:08, thread 222, GC) are 'CAS ref catalog erase ... gave up at the policy deadline after zero attempt(s)': CasWriteResult.h renders 'zero' when GaveUp::Deadline fires with sent_any=false, and completedRemovingDelete (CasRefCatalog.cpp) freezes ONE Retry::standard bound for the whole erase/resolve loop, so a loop that spent its 90 s on earlier contended iterations refuses the next erase before sending it. No storage error of any kind in the 100 s before 02:00:08 (only SSL client noise), so this event is catalog contention between the three alter-cas nodes, not RustFS; GC retried on the next tick. Trace-level evidence is unavailable: clickhouse-server.log rotated at 02:10:18.

2026-11-11 PR https://github.com/Altinity/ClickHouse/pull/2805 @3fa3a01203b (run 42921139681): `RegressionTestsAarch64 / CASS3CacheAlter (attach, 3)` failed 2 leaf scenarios of 3427. One `INSERT` got `CAS _ckpt for namespace ...: gave up at the policy deadline after zero attempt(s)` at 03:03:39 job time (one such line in the job log); the other scenario prints only `AssertionError`. The job was green on the previous run of the PR (42864462634). RustFS log not checked for this run.
<!-- SECTION:NOTES:END -->
