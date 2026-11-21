---
id: CAS-121
title: Add the HTTP `/ping` healthcheck to `ch2` in the ca-soak compose files
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:soak'
  - 'complexity:trivial'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:issue'
  - quick-win
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
references:
  - utils/ca-soak/docker-compose.yml
  - 'https://github.com/Altinity/ClickHouse/issues/2233'
priority: low
type: chore
ordinal: 159000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Only `ch1` has the `/ping` healthcheck (`utils/ca-soak/docker-compose.yml:91-96`, added to serialize capability probes);
`docker-compose-asan.yml` and `-tsan.yml` have none. So "container healthy while HTTP dead" on `ch2` is vacuous, which is how
issue #2233's report read. Trivial.

Provenance: BACKLOG/operability-and-introspection.md#issue-2233-followups item (1); verified 2026-09-26 against dd0ed2f263a.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every ca-soak compose file defines the same HTTP healthcheck on every ClickHouse service
- [ ] #2 `docker compose ps` reports `ch2` unhealthy within one probe interval after its HTTP port stops answering
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
First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

Identifier trace: the earliest docs mention of `docker-compose-asan.yml` is 2026-08-04 (6cbeec21537); the finding itself has no record before this migration, so the creation date stays 2026-09-26.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: grep -c healthcheck: docker-compose-asan.yml 0, docker-compose-tsan.yml 0; other compose files have one. Changes: label quick-win.
---
<!-- COMMENTS:END -->
