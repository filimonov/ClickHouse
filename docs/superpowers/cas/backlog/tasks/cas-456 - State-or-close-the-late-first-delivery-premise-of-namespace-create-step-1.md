---
id: CAS-456
title: State or close the late-first-delivery premise of namespace-create step 1
status: To Do
assignee: []
created_date: '2026-11-11 17:18'
labels:
  - 'area:ref-catalog'
  - 'origin:review'
  - 'risk:low'
dependencies:
  - CAS-368
priority: low
type: bug
ordinal: 560000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After CAS-368 step 1 of namespace create never resends a catalog row. Its first attempt can still be delivered late. A delayed conditional PUT of row x that expects etag E0 can land after another incarnation y was created, completed, removed and erased, when the catalog bytes equal E0 again. This needs a content-derived etag (the S3 dialect); the GCS-generation and emulated dialects mint etags and are not exposed. Result: a resurrected `Creating` row owned by a live fence. It blocks ref writes to that namespace (`checkPublicationAdmittedOrThrow`) until reconcile or the same-fence resume completes it. The exposure is bounded by the store's request lifetime and predates CAS-368. Two premises are unstated in `CA/Pool/CasRefCatalog.cpp`: the store applies a conditional PUT at most once, and only within a bounded time after the client gave up. Decide: state both premises in the code and docs, or add a revision field to the catalog body so equal bytes never give an equal etag (format change, frozen since 26.6.4).
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
