---
id: CAS-467
title: >-
  The ca-soak S3 fault proxy caps throughput at about 17 INSERT/s and distorts
  latency in every scenario that runs through it
status: To Do
assignee: []
created_date: '2026-11-11 23:41'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:soak'
  - 'area:testing'
  - 'area:tooling'
  - 'impact:operability'
  - 'needs:decision'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: low
type: task
ordinal: 571000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`utils/ca-soak` fault proxy `s3_fault_proxy.py` is a threaded Python server that opens a new upstream connection per request. Measured 2026-11-11 (master/tmp/cas297/runs/mt100_proxy_saturated against a direct rerun): 16 writers reach 17 INSERT/s through it with p50 0.18 s, synchronized 10-45 s stalls and 10702 `502 Bad Gateway` answers (10485 CAS reissues); with the disk pointed at RustFS directly the same load gives 201 INSERT/s, p50 0.03 s, no reissue. Every soak card that uses `docker-compose-s3faultproxy.yml` measures the proxy when it reports latency, GC round time or request counts; fault-injection outcomes (error counts, lease events) are not affected. Options: keep-alive upstream connections and a process pool, or replace it with toxiproxy for hang/latency faults and keep the Python proxy for the semantic faults only.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A soak scenario through the proxy reaches at least half the direct INSERT rate, or scenarios that report latency bypass it
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
2026-11-12 correction after an independent check of the report against run data (docs/superpowers/reports/2026-11-11-cas-defaults-review.check.md.txt, a49f8a8e45f): figures in the description: 201 INSERT/s is the best minute of the direct run, the run average is 173 INSERT/s; the count of 502 answers is about 11000 lines in one server error log, not an exact proxy counter. The inflation of latency and per-log GC cost through the proxy is 10-15 times.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: utils/ca-soak/proxy/s3_fault_proxy.py:251 still opens a fresh http.client.HTTPConnection(UPSTREAM) per request inside a ThreadingMixIn server (:446); no connection reuse or pool.. Changes: impact:operability; label needs:decision. Owner question: Keep-alive plus process pool in the Python proxy, toxiproxy for hang/latency faults, or have latency-reporting scenarios bypass the proxy?
---
<!-- COMMENTS:END -->
