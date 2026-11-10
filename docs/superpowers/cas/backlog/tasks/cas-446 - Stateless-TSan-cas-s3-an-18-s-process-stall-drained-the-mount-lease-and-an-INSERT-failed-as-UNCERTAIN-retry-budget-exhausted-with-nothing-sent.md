---
id: CAS-446
title: >-
  Stateless TSan cas-s3: an 18 s process stall drained the mount lease and an
  INSERT failed as UNCERTAIN (retry budget exhausted) with nothing sent
status: To Do
assignee: []
created_date: '2026-11-10 22:56'
labels:
  - 'area:write-path'
  - 'area:mounts'
  - 'area:ci'
  - 'origin:ci'
dependencies: []
references:
  - >-
    https://altinity-build-artifacts.s3.amazonaws.com/json.html?PR=2801&sha=3963b4f40ecff91f8b657adc0f820918c29e9626&name_0=PR
  - 'https://github.com/Altinity/ClickHouse/pull/2801'
priority: medium
type: bug
ordinal: 549000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR 2801 @3963b4f40ec, job `Stateless tests (amd_tsan, cas s3 storage, parallel, 1/2)`, test `01171_mv_select_insert_isolation_long`: `INSERT INTO tmp SELECT 2, nm FROM mv` failed with `Code: 210 ... stageManifest: part-manifest PUT at '.../0000000000000001-0000000000003293/000001.zst' is UNCERTAIN (retry budget exhausted) — nothing conclusive was named`. Logs: tmp/review_2801/regression/st01171/ (server.log, server.err.log, job.log).

Timeline (server log, UTC):
- 15:37:04.47 part `all_326_326_0` renamed; `stageManifest` starts on thread 91484.
- 15:37:05–15:37:23 the whole server logs 1 line per second (391/s before, 447/s after): an 18 s process-wide stall. Nothing in server.log, err.log or stderr explains it. Tests running concurrently (`03041_recursive_cte_postgres_7`, `00709_virtual_column_partition_id`) took 30 s instead of a few seconds.
- 15:37:13.2 the write fence refuses admissions: `mount_lease_ttl_ms` 30 s, `mount_renew_period` 10 s, attempt envelope 7 s + `lease_safety_margin_ms` 2 s; the renewal due during the stall never landed (defaults; tests/config sets none of them).
- 15:37:23.06 `stageManifest` gives up with `GaveUp{Deadline, Lease, sent_any=false}`, thrown as `CasWriteRetryLater` (210 NETWORK_ERROR) and surfaced by `MergeTreeSink` to the client. 15:37:23.07 `CasPool` warns: writes paused since 9857 ms, last renewal error none.
- 15:37:25.04–15:37:27.79 the renewal PUT (320 bytes) takes 2.75 s. 15:37:30 four background tasks fail with `no lease budget for one more request`, then everything recovers. One incident in the 2.5 h job.

Two defects and one open question:
1. Message. `Pool/CasPartWriteTxn.cpp:682` maps every `GaveUp` to "UNCERTAIN (retry budget exhausted)" and ignores `why`, `deadline_source` and `sent_any`. Here nothing was sent (the fence refused before the first attempt), so the outcome is certain and the cause is the lease, not the retry policy. Same pattern at `Tools/CasDecommission.cpp:492`.
2. A process stall of 10 s or more under a 30 s lease is a client-visible INSERT failure. CAS-405.3 (120 s lease, 40 s renew) absorbs this one; CAS-185 is the wait-for-remount design question.
3. What stalls the TSan stateless server for 18 s is not visible from inside the process; the sanitizer lane profile is CAS-223.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `stageManifest` and the decommission tombstone write report `GaveUp` by its fields: 'not sent: the mount lease paused writes N ms ago' when `sent_any` is false, 'UNCERTAIN' only after an attempt was sent; a gtest covers both
- [ ] #2 Decide and record which of CAS-405.3 and CAS-185 closes the client-visible failure on a short process stall; link the decision here
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
