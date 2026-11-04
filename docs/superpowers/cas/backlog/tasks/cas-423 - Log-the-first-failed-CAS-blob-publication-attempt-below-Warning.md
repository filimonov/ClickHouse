---
id: CAS-423
title: Log the first failed CAS blob publication attempt below Warning
status: Done
assignee: []
created_date: '2026-10-24 12:24'
updated_date: '2026-11-04 15:00'
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
- [x] #1 The first failed publication attempt is logged below Warning; attempt 2 and later and the give-up path stay at Warning
- [x] #2 Test `CASBlobPublishTransport.FirstFailedAttemptIsBelowWarning` (or equivalent) pins the level
- [x] #3 Stateless cas s3 lanes stop failing on the `Publication of CAS blob ... attempt 1 of 8` line
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [x] #2 ASan lane green for touched suites
- [x] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [x] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-04: merged into antalya-26.6 as aedac0cf3f7 (https://github.com/Altinity/ClickHouse/pull/2594). The 4 reds on its last run were not this line: Grype x2, `Stateless tests (amd_binary, cas storage, parallel)` died on `GroupingAggregatedTransform pushed bucket A twice`, Unit tests (asan_ubsan) unrelated. AC#3: the cas lanes no longer fail on `Publication of CAS blob ... attempt 1 of 8`.
<!-- SECTION:NOTES:END -->
