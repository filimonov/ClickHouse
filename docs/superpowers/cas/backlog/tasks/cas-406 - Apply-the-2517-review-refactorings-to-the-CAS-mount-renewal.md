---
id: CAS-406
title: 'Apply the #2517 review refactorings to the CAS mount renewal'
status: To Do
assignee: []
created_date: '2026-10-18 20:59'
updated_date: '2026-10-18 21:13'
labels:
  - 'area:mounts'
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2517'
priority: low
type: task
ordinal: 509000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up to https://github.com/Altinity/ClickHouse/pull/2517 (approved by k-morozov with refactoring comments, no behaviour change). (1) rename MountLeaseRenewer::renewUntilDefinitive to renewUntilResolved; (2) say in the name or comment of Retry::untilLeaseSafe that its bound is an absolute lease deadline; (3) replace the bool until_definitive in MountLeaseRenewer::renewOn with a policy, as CasOperation::writeLoop does, and derive the Retry choice from it; (4) handle WriteResult alternatives with an exhaustive std::visit so a new alternative fails to compile; (5) rename the open_requests plane to lease_requests; (6) rename Retry::attempt_spacing_ms / kDefinitiveAttemptSpacingMs to reissue_interval_ms. CasMountRuntime.cpp:620 (requests used inside methods are not obvious): no concrete proposal; leave and answer on the PR.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 No behaviour change: CAS* release + ASan gates green, models untouched
- [ ] #2 Each review comment answered on #2517 with the commit that addresses it, or why not
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
Central change (review comment at CasMountRuntime.cpp:620, also covers comments 1, 3, 5): the renewal's request plane and retry policy are decided in three places — the method name (renew = fenced mount_requests, renewForRemount and renewUntilDefinitive = open_requests; CasServerRoot.cpp:1620-1632), the until_definitive flag in renewOn, and renewalEnvironment(worker_call) — so the caller at CasMountRuntime.cpp:620-624 cannot see that the background renewal runs outside the mount fence while the startup one runs under it. Replace the three methods and the flag with one renew(MountRenewMode, environment), enum MountRenewMode { Bounded (fenced plane, untilLeaseSafe: startup, decommission admin), Remount (lease plane, untilLeaseSafe: fence still latched lost), UntilResolved (lease plane, until a definitive answer: background) }. The renewer maps mode to (plane, Retry) in one switch without default; the runtime maps RenewalDriverState and bounded_background_renewal to a mode in one switch. Callers still name a situation, never a plane. No behaviour change.
<!-- SECTION:NOTES:END -->
