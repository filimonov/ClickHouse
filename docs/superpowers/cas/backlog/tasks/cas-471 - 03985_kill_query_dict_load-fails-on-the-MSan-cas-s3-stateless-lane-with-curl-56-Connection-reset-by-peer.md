---
id: CAS-471
title: >-
  03985_kill_query_dict_load fails on the MSan cas-s3 stateless lane with curl
  (56) Connection reset by peer
status: To Do
assignee: []
created_date: '2026-11-12 21:36'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
  - 'confidence:speculative'
  - groomed-20261121
  - 'groom-20261121:unclear'
  - 'groom-20261121:certainty-low'
  - grooming-20261121-changed
dependencies: []
priority: low
ordinal: 576000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Job 131345479210 (MSan cas s3 storage 2/3): FAIL after 128 s, stderr 'curl: (56) Recv failure: Connection reset by peer' during KILL QUERY over HTTP while clickhouse-test shed workers at 82 to 88 percent server memory. The test uses no table. Not seen on base runs 43246617309 and 43091569224. Cause unknown; CAS relation unknown. One occurrence: root-cause when it repeats or when the lane uploads server logs on timeout. PR https://github.com/Altinity/ClickHouse/pull/2831 @d8cbdfae3ec, run 43248330323; triage tmp/prbundle/wave1/ci/triage.md.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: unclear (low). Evidence: tmp/prbundle/wave1/ci/triage.md: one occurrence (job 131345479210), test touches no table, failed while workers shed at 82-88% server memory, not on either base run; ci2 triage shows no repeat. Changes: confidence set to speculative. Links: related: CAS-223 (MSan cas s3 shards time out, shedding). Would settle it: a second occurrence or the server log of job 131345479210 (timed-out jobs upload none).
---
<!-- COMMENTS:END -->
