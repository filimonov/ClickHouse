---
id: CAS-456
title: State or close the late-first-delivery premise of namespace-create step 1
status: To Do
assignee: []
created_date: '2026-11-11 17:18'
updated_date: '2026-11-21 08:21'
labels:
  - 'area:ref-catalog'
  - 'origin:review'
  - 'risk:low'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies:
  - CAS-368
priority: low
type: bug
ordinal: 560000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After CAS-368 step 1 of namespace create never resends a catalog row. Its first attempt can still be delivered late. A delayed conditional PUT of row x that expects etag E0 can land after another incarnation y was created, completed, removed and erased, when the catalog bytes equal E0 again. This needs a content-derived etag (the S3 dialect); the GCS-generation and emulated dialects mint etags and are not exposed. Result: a resurrected `Creating` row owned by a live fence. It blocks ref writes to that namespace (`checkPublicationAdmittedOrThrow`) until reconcile or the same-fence resume completes it. The exposure is bounded by the store's request lifetime and predates CAS-368. Two premises are unstated in `CA/Pool/CasRefCatalog.cpp`: the store applies a conditional PUT at most once, and only within a bounded time after the client gave up. Decide: state both premises in the code and docs, or add a revision field to the catalog body so equal bytes never give an equal etag (format change, frozen since 26.6.4).

Update 2026-11-21: Partly stated already: `CasRefCatalog.cpp:305-307` records the 'store applies one conditional write at most once' premise. The 'only within a bounded time after the client gave up' premise and the docs side are still unstated.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A written decision: accept the request-lifetime bound and state both premises in `CasRefCatalog.cpp` and the CAS docs, or add a catalog revision field
- [ ] #2 If the revision field is chosen: a gtest delivers a first step-1 attempt after an erase of a later incarnation and sees the write refused
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
created: 2026-11-21 08:21
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Pool/CasRefCatalog.cpp:298-310 states the at-most-once-conditional-write premise for createNamespaceStep1, not the bounded-time-after-client-gives-up premise; no catalog revision field; the decision is still unwritten. Changes: impact:availability; description: Update paragraph appended. Links: CAS-368 is still To Do in the backlog although the code comment at CasRefCatalog.cpp:302 already describes minting a fresh incarnation per decision (resend_unresolved = false); worth checking CAS-368 for closure. Would settle it: whether docs/en/antalya/cas states either premise (not searched).
---
<!-- COMMENTS:END -->
