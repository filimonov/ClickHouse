---
id: CAS-403
title: Do not throw LOGICAL_ERROR from installRenewer when a stop races a remount
status: To Do
assignee: []
created_date: '2026-10-16 00:29'
labels:
  - 'area:mounts'
dependencies: []
priority: medium
ordinal: 501000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found by the TLA run for PR #2517 (model CaRenewalDriver, config finding_install_after_stop, branch models/mount-lease-series-v2 @1e4f3222015). Pre-existing at altinity/antalya-26.6-20261008: the remount worker is in Pool::tryRemountOnce after the claim when stopBackgroundWorkers sets the driver to Stopping; the next installRenewer throws LOGICAL_ERROR (it requires Dormant/Parked). remountLoop catches it in release builds; debug and sanitizer builds abort. Not reproduced by a test yet.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest reproduces the stop-during-remount race and the remount ends without LOGICAL_ERROR (death-test split under DEBUG_OR_SANITIZER_BUILD for the red run)
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
