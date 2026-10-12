---
id: CAS-377
title: >-
  Check whether relink or move trusts a source manifest snapshot that predates
  the destination precommit
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:replication'
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:speculative'
  - 'risk:high'
  - 'needs:repro'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-blob.md
priority: medium
type: research
ordinal: 471000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Codex (inferred): the source resolve/manifest snapshot precedes the destination precommit; another actor drops the source and GC deletes its blobs; promotion trusts the retained decoded entries, which are memory, not durable ownership. Caller-level exclusion would need proving. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-blob.md (finding 3).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 verdict with the interleaving or the excluding happens-before; task filed if reachable
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
