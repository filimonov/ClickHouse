---
id: CAS-446
title: >-
  Stateless TSan cas-s3: an 18 s process stall drained the mount lease and an
  INSERT failed as UNCERTAIN (retry budget exhausted) with nothing sent
status: To Do
assignee: []
created_date: '2026-11-10 22:56'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:write-path'
  - 'area:mounts'
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
  - 'needs:decision'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
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

Update 2026-11-21: Remaining scope after PR 2831 (0aa837e942e): (1) decommission tombstone give-up text from the give-up facts, including the pause age; (2) re-decide the client-visible failure on a process stall at the 40 s default lease (CAS-405.3 vs CAS-185). The part-manifest text is done (06f74c43a1f).
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-11 defaults review: process-stall runs (`docker pause` of both servers): at 30/10 a 20 s stall costs 4 INSERT and 1 SELECT errors at wake-up; at 120/10 stalls of 20 s and 45 s cost none and 90 s costs 0-1. So the lease default closes the 18 s CI case; the misleading UNCERTAIN text (AC #1) stays. Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (worktree ~/workspace/ClickHouse/cas-297-defaults); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12. AC #2 is a decision, not code: the 120 s default lease of CAS-405.3 (wave 1 of the 26.8 bundle, spec `docs/superpowers/specs/2026-11-12-cas-improvements-wave1-design.md` section 6) removes the client-visible failure of a short stall; at 120/10 the 20 s and 45 s process stalls of `docs/superpowers/reports/2026-11-11-cas-defaults-review.md` cost no errors. AC #1: the decommission tombstone write already reports by `sent_any` (`Tools/CasDecommission.cpp:95-106`); the one remaining text is the part-manifest give-up at `Pool/CasPartWriteTxn.cpp:697-702`, rewritten in the wave from the `GaveUp` fields (six cases, spec section 10.4).

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commit 06f74c43a1f "Build the manifest give-up text from the give-up facts" covers: #1 for the part-manifest `PUT` in `stageManifest`: six texts chosen by `why`, `sent_any` and `deadline_source`, attempt count only when the body may have landed; `CASPartWriteTxn.StageManifestNotSentUnderDrainedLeaseSaysNotSent` and `CASPartWriteTxn.StageManifestSentThenGaveUpSaysUncertain` cover both. Still open: #1 for the decommission tombstone write, which already reports by `sent_any` only and is unchanged, and no text carries the pause age; #2 the decision (CAS-405.3, the 120 s lease, closes the short-stall failure) is recorded in the task, but that default is under an open decision (CAS-480, restart wait about 131 s on the head, mixed-build pools need 30 s per CAS-475), so the decision holds only while the default stays. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. `06f74c43a1f` builds the `stageManifest` give-up text from the give-up facts. The decision "CAS-405.3 closes the stall" assumed the 120 s lease; at the 40 s default the write window is 14 s. Re-decide against CAS-185.

2026-11-17: The default mount lease is 40 s since PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit feb86da1f4a (30 s -> 120 s -> 40 s). Earlier notes that name 120 s no longer describe the default. At 40 s a restart after a kill waits about 47 s and the write window in a store outage is 14 s. The decision "CAS-405.3 closes the 18 s stall" assumed 120 s; it must be taken again.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: 06f74c43a1f is an ancestor of the checked branch and its gtests exist (src/Disks/tests/gtest_cas_part_write.cpp: StageManifestNotSentUnderDrainedLeaseSaysNotSent), so AC1 is done for the part-manifest PUT; the decommission tombstone text and the AC2 decision (default lease is now 40 s, not 120 s) remain open per the task notes. Changes: label needs:decision; description: Update paragraph appended. Links: related CAS-405.3, CAS-185, CAS-480, CAS-223. Would settle it: whether the decommission tombstone write still reports by sent_any only (Tools/CasDecommission.cpp) and the owner's re-decision between CAS-405.3 and CAS-185 at the 40 s default. Owner question: At the 40 s default lease the write window is 14 s: does a process stall of 10 s or more stay a client-visible INSERT failure (accept), or does CAS-185 (wait-for-remount) close it?
---
<!-- COMMENTS:END -->
