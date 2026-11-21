---
id: CAS-407
title: >-
  Move the CAS mount renewal loop into the request engine and check whether one
  request plane suffices
status: To Do
assignee: []
created_date: '2026-10-18 21:13'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:mounts'
  - 'complexity:medium'
  - 'risk:high'
  - 'confidence:speculative'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies:
  - CAS-406
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2517'
priority: low
type: research
ordinal: 510000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Review simplification 6 of https://github.com/Altinity/ClickHouse/pull/2517: the until-definitive loop in MountLeaseRenewer::renewOn (refusal pause, read-to-observe, same-tuple reissue) belongs in CasOperation's write loop. Open question to settle first on the models: the startup and bounded (decommission admin) renewals run on the fenced mount_requests plane, while the remount re-anchor already runs on the open lease plane bounded only by untilLeaseSafe and its liveness predicate. If CaMountRenewRetryCore and CaRenewalDriver show that the liveness predicate covers what the mount fence adds for startup (generation and lost checks), the renewer can drop mount_requests and use one plane. That is a behaviour change: model first, then code. After CAS-406.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Model result: startup renewal on the lease plane keeps LateSuccessNeverOpensWrites and NoAuthorityAfterEntitledTakeover, or a counterexample shows why the fenced plane is needed
- [ ] #2 If one plane suffices: renewer holds one CasRequests; else a comment at the mode table states what the fence adds
<!-- AC:END -->

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
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: task text; CAS-406 (declared dependency) not complete: renewUntilDefinitive still in CasMountRuntime.cpp; no model result recorded. Changes: size unset -> medium (M): model run plus a behaviour-changing refactor; risk set to high; confidence set to speculative; type task -> research: first deliverable is a model check, code only if it passes. Size: M. Would settle it: model result for AC1 (CaMountRenewRetryCore / CaRenewalDriver on one plane).
---
<!-- COMMENTS:END -->
