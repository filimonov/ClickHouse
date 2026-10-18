---
id: CAS-405
title: >-
  Choose CAS defaults that survive a two-minute object-store outage without a
  lost mount
status: To Do
assignee: []
created_date: '2026-10-18 13:52'
labels:
  - 'area:mounts'
dependencies: []
priority: high
ordinal: 503000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Goal: losing connectivity to the object store for about 2 minutes passes without a lost mount, without a remount and without client-visible write errors beyond a pause. Today: cas_mount_lease_ttl_ms 30 s, writes give up at the 90 s policy deadline, an attempt is bounded by the disk request_timeout_ms (30 s); RustFS read stalls of 90-100 s already fail CI (#2343). #2516 (observers wait for the declared lease, holder cap 10 min) makes a longer lease safe; #2517 turns an expired lease into a pause. Decide lease TTL and renew period, the write policy deadline, attempt timeouts; cost: slower detection of a dead member, longer GC floor hold. Context: docs/superpowers/cas/2026-10-18-open-items.md#defaults-two-minute-outage.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 On the fault-proxy stand with the store unreachable for 120 s under inserts, merges and selects: no CASMountLeaseLost, no remount, no client error other than a documented pause; new defaults documented in configuration.md
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
