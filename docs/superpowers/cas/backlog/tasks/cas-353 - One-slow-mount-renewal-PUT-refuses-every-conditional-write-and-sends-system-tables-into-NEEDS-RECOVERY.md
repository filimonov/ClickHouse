---
id: CAS-353
title: >-
  One slow mount-renewal PUT refuses every conditional write and sends system
  tables into NEEDS RECOVERY
status: To Do
assignee: []
created_date: '2026-10-08 20:51'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:lease'
  - 'area:ux'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:ci'
  - 'theme:ci-stabilization'
dependencies: []
priority: high
type: bug
ordinal: 447000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
With the defaults (TTL 30 s, period 10 s, write reserve 2 x 7 s + 2 s), the slack between a due renewal and the write cutoff is 4 s. On PR #2513 (asan cas-s3 2/2, 2026-10-08 18:54:57) all S3 traffic on the server stalled for about 6.5 s. The renewer was blocked in `WriteBufferFromS3::finalizeImpl` for 7 consecutive `trace_log` samples (18:54:57.29 to 18:55:03.27). Conditional writes were refused from 18:55:00.76 to 18:55:03.5: test `CREATE TABLE` (`03595_parallel_replicas_join_remote`) failed with "gave up at the lease deadline after zero attempt(s)" (Code 210, 18:55:02.168), a `DROP` was deferred (18:55:02.348), and two system-log ref tables went to `NEEDS RECOVERY` (18:55:01.047, 18:55:01.050). The same class shows on #2512 1/2 at 18:03:04.185 (`CasWriteRetryLater`, no PR 4 code). The message says "lease deadline" while about 14 s of lease remain, which misleads the reader.

Evidence for the deferred series items 17b/18 (admission by request deadline): with TTL 30 s, period 10 s and a 16 s reserve the slack is 4 s; the renewal itself was blocked 6 s in `WriteBufferFromS3::finalizeImpl` -> `TaskTracker::waitAll` (S3 stall), no observer frame; same refusal on #2512 which lacks PR 4.

Options: widen the slack (renew earlier, or reserve one envelope for the renewal's own lane), or change the message to name the write reserve rather than the deadline. Related: docs/superpowers/specs/2026-10-02-cas-mount-lease-settings-ux-notes.md, #2431.

Evidence: tmp/lease-series/rca/ci-rca-2513-2510.md (Item 1); PR https://github.com/Altinity/ClickHouse/pull/2513 and https://github.com/Altinity/ClickHouse/pull/2512.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Writes are not refused by a single renewal PUT that takes less than the renewal period
- [ ] #2 The reserve/slack relation is printed at mount (series PR 9b) and documented
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
2026-11-11 defaults review: reproduced on the stand: every PUT delayed 6 s for 60 s gives 58 INSERT errors from 16 s at 30/10 (`def_slowput6`) and none at 120/40 or 120/10 (`l120_slowput6`, `rec_slowput6`); a 3 s delay gives none at 30/10. The 4 s slack is the cause; a 120 s lease with a 10 s renew period makes the slack 94 s. Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (worktree ~/workspace/ClickHouse/cas-297-defaults); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 (branch head a3967637843, draft, not merged). Commits b528b234dca "Default the CAS mount lease to 120 s" and 9dbc97f3e19 "Print the write reserve and the renewal slack at mount" cover: #1 with 120 s lease and 10 s renew period the slack between a due renewal and the write cutoff is 94 s, so a renewal `PUT` shorter than that does not refuse writes; #2 `write_reserve_ms` and `renewal_slack_ms` are in the mount request budget log line, documented in `mounts-and-leases.md`. Still open: #1 for a renewal `PUT` slower than 94 s, because background renewal retries with `Retry::untilDefinitive` and no request window bounds it (no owner named). The 120 s default is under an open decision (CAS-480): CI of the head shows a restart after a hard kill waits about 131 s (36.5 s at 30 s), and a mixed-build pool needs the lease pinned to 30 s (CAS-475). The default may be reverted or pinned in tests, which would restore the 4 s slack. CI run https://github.com/Altinity/ClickHouse/actions/runs/43379266824. Evidence on the head tree: `CAS*` gtests 3052 passed (release) and 3056 (ASan), 59 of 59 `*_cas_*` stateless tests.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. `9dbc97f3e19` prints the reserve and the slack at mount. With the 40 s default (`feb86da1f4a`) a renewal PUT shorter than 14 s does not refuse writes (4 s at 30 s). A renewal PUT slower than that is still unbounded by a request window.

2026-11-17: The default mount lease is 40 s since PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit feb86da1f4a (30 s -> 120 s -> 40 s). Earlier notes that name 120 s no longer describe the default. At 40 s a restart after a kill waits about 47 s and the write window in a store outage is 14 s.
<!-- SECTION:NOTES:END -->
