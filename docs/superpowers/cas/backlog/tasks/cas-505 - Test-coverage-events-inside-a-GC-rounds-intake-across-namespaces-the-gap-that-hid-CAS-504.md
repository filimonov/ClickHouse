---
id: CAS-505
title: >-
  Test coverage: events inside a GC round's intake, across namespaces (the gap
  that hid CAS-504)
status: To Do
assignee: []
created_date: '2026-11-19 11:48'
updated_date: '2026-11-21 08:24'
labels:
  - cas
  - gc
  - tests
  - coverage
  - 'area:testing'
  - 'complexity:epic'
  - 'risk:low'
  - 'confidence:solid'
  - 'impact:data-loss'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: high
type: task
ordinal: 610000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
CAS-504 (a body deleted while a no-bytes copy in another namespace names it) passed 3,500 unit tests, 19 integration suites, 46 scenario cards, two soaks, real-cloud runs, five model families and about twenty reviews. What was missing: (1) no test places an event BETWEEN the fold's reads of two namespaces: unit tests drive a round as one step, with hooks only at the cut boundary and at decision HEADs; (2) no test combines a cross-namespace no-bytes copy (`MOVE PARTITION TO TABLE`, cross-namespace `RENAME TABLE`, `ATTACH PARTITION FROM`, `REPLACE PARTITION`, replica fetch by relink) with a drop of the source and a running GC whose row for that blob is already `pending`; (3) the models had one namespace or one consistent cut: the per-namespace frontier was never modelled; the spec's argument for no-bytes copies (section 4.2) assumed a consistent cut without listing it as an assumption; (4) the soaks and scenario cards exercise insert / drop / truncate / mutate under GC, not cross-table moves of revived content; (5) the protocol change shortened condemn-to-delete from three zero cuts to two and no review asked what the third cut had protected. To do: a fold test facility that runs a callback between namespace reads (both walk orders, every pair of adjacent reads), and a matrix: {each operation that adds a reference without reading the body} x {source dropped: before / inside / after the intake} x {row state: none, candidate, pending, standing} x {one hop, relay}; a soak phase and a scenario card with cross-table moves of hot content under continuous GC; the model of record with at least two namespaces and independent frontiers; the same audit for other 'inside a round' interleavings (namespace birth and removal, ref-log snapshot, intake slice cut, rebuild, decommission). Related: CAS-504.

Update 2026-11-21: Part (1) of the gap is covered: a hook between the two namespace walks of pass 1 exists (`CASGcRoundPoint.BetweenNamespacesInPassOneFiresBetweenTheTwoWalks`) and the cross-namespace repro tests are committed. Remaining scope: the full operation x drop-timing x row-state x hops matrix, a soak phase and scenario card for cross-table moves under GC (the `intents` soak profile has MOVE/ATTACH copies), the multi-namespace model, and the audit of the other inside-a-round interleavings. Split into one story per bullet before work.
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
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: cas-pr1 src/Disks/tests/gtest_cas_gc_intents.cpp:5371 TEST(CASGcRoundPoint, BetweenNamespacesInPassOneFiresBetweenTheTwoWalks): a between-namespaces hook exists; cross-namespace repro tests are committed (4e478a0ce9f, 2389c55c9f5). Changes: area set to testing; size unset -> epic (XL): matrix, soak, model and audit: must be split; risk set to low; confidence set to solid; impact:data-loss; type unset -> task: test coverage work in several parts; description: Update paragraph appended. Size: XL. Links: Related: CAS-504 (the defect), CAS-507. Would settle it: How much of the matrix (operation x drop timing x row state x hops), the soak phase, the scenario card and the two-namespace model is already done.
---
<!-- COMMENTS:END -->
