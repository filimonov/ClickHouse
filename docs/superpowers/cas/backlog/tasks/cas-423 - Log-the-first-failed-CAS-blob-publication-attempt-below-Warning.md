---
id: CAS-423
title: Log the first failed CAS blob publication attempt below Warning
status: In Progress
assignee: []
created_date: '2026-10-24 12:24'
labels:
  - 'area:write-path'
  - 'origin:review'
milestone: m-8
dependencies:
  - CAS-361
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2594'
priority: high
type: bug
ordinal: 526000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The per-attempt `CasBlobPublishAttempt` WARNING added by https://github.com/Altinity/ClickHouse/pull/2555 (CAS-361) reaches clients with `send_logs_level=warning` and failed two stateless tests on stderr in `stateless asan cas s3 2/2`: `02907_filter_pushdown_crash` and `03254_pr_join_on_dups`. Both saw `Publication of CAS blob ... failed, attempt 1 of 8: ... Connection refused`; attempt 2 succeeded. Fix: log the first failed attempt at Information; attempt 2 and later and the give-up path stay at Warning; `CASBlobPublishAttemptFailed` and `CASBlobPublishGaveUp` unchanged. PR: https://github.com/Altinity/ClickHouse/pull/2594 (open 2026-10-17; CI had 4 red checks on 2026-10-24, to be triaged). Related: CAS-402 (which harnesses fail on server log lines), CAS-354 (same class on a conditional PUT).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The first failed publication attempt is logged below Warning; attempt 2 and later and the give-up path stay at Warning
- [ ] #2 Test `CASBlobPublishTransport.FirstFailedAttemptIsBelowWarning` (or equivalent) pins the level
- [ ] #3 Stateless cas s3 lanes stop failing on the `Publication of CAS blob ... attempt 1 of 8` line
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
