---
id: CAS-197
title: >-
  Audit CAS etag comparisons for quoting and weak-tag differences across S3, GCS
  HMAC and GCS OAuth
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:20'
labels:
  - 'area:backend'
  - 'area:gcs'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:speculative'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
references:
  - R/Backend/CasEtag.h
  - R/Backend/CasEtag.cpp
priority: low
type: research
ordinal: 254000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A compatibility review against entity-tag grammar was flagged on 2026-09-02 and never filed; its scope was not recorded.
`PersistedEtag::matches` compares renderings textually (`R/Backend/CasEtag.h:85-88`), and `isIncarnationValue` accepts any non-empty, non-`*`, comma-free value (`R/Backend/CasEtag.cpp:10-27`). Nothing normalizes quotes or a `W/` prefix.
Questions: does each store return the same quoting on HEAD, GET and PUT responses? Can any return a weak tag? Do persisted tokens in manifests survive a store that changes quoting?
Persisted tokens are on-S3 format: a normalization change falls under decision-4.

Provenance: BACKLOG/ref-protocol.md#entity-tag-grammar-compat-watch (from random/todo.md item 14). Verified 2026-09-26 against b1c34d03479 and 0dbbd797792.

Update 2026-11-21: CasEtag.cpp now handles quoting and the W/ prefix (see CasEtag.cpp:49-129), so the premise that nothing normalizes them is out of date. Re-scope to the remaining question: record per backend the etag form on HEAD, GET, PUT and If-Match responses (S3, GCS HMAC, GCS OAuth) and check the new parser against each.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A short note records, per backend, the etag form on HEAD, GET, PUT and `If-Match` responses, from live runs or vendor docs
- [ ] #2 Any mismatch found becomes a follow-up task; none found closes this
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
First recorded: 2026-09-26 (cdbde71edf2, by 'entity-tag-grammar-compat-watch')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: cas-pr1 Backend/CasEtag.cpp:49-129 now parses quoted strong ETags, rejects a leading W/ as weak and quotes a tag that starts with W/; the premise 'nothing normalizes quotes or W/' is stale. Changes: type spike -> research: an investigation with a recorded note as output; description: Update paragraph appended. Pointers stale: CasEtag.h:85-88 and CasEtag.cpp:10-27 (isIncarnationValue) no longer match: the file was reworked to parse strong ETags. Would settle it: the per-backend note of HEAD/GET/PUT/If-Match etag forms (AC1) was not found; unclear how much of the grammar question the rework covers.
---
<!-- COMMENTS:END -->
