---
id: CAS-353
title: >-
  One slow mount-renewal PUT refuses every conditional write and sends system
  tables into NEEDS RECOVERY
status: To Do
assignee: []
created_date: '2026-10-08 20:51'
labels:
  - 'area:lease'
  - 'area:ux'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:ci'
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
