---
id: CAS-130
title: Remove internal plan and backlog tags from source comments in one pass
status: To Do
assignee: []
created_date: '2026-08-22'
updated_date: '2026-11-21 08:23'
labels:
  - 'area:upstream'
  - 'complexity:medium'
  - 'risk:low'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-4
dependencies: []
references:
  - src/Common/ProfileEvents.cpp
  - src/Common/ThreadStatus.h
  - src/Disks/tests/gtest_ca_wiring.cpp
priority: low
type: chore
ordinal: 169000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
About 239 lines in 72 files under `src`/`programs` carry `B<number>` tags, 60 files outside `CA/` (regex over-matches, treat as
approximate), e.g. `src/Common/ProfileEvents.cpp:789` (B168), `src/Common/ThreadStatus.h:91` (B90). Eight `M-W` plan
references remain in `src/Disks/tests/gtest_ca_wiring.cpp:31,332,875,1029,1468,1520,1632` and `gtest_cas_layout.cpp:126`.
Policy: keep the reason, drop the provenance. One sweep, not a fix round per site; P2 because most hits are in shared code.

Provenance: BACKLOG/operability-and-introspection.md#internal-provenance-ships (opus review M8) and #b131-repo-hygiene-mw-sweep ([B131]); verified 2026-09-26 against dd0ed2f263a and 0dbbd797792.

Update 2026-11-21: Most B-tags were swept already: 7 files with bracketed B-tags remain on this branch (e.g. src/IO/S3Defines.h, src/IO/S3/PocoHTTPClient.h, src/IO/S3/tests/gtest_aws_s3_client.cpp) plus M-W references in gtest_ca_wiring.cpp (:31 seen). Re-run the sweep to size the rest.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 No comment under `src`/`programs` cites a `B<number>`, `M-W` or `D-W1` tag, and each edited comment still states its reason
- [ ] #2 The sweep lands as one commit that touches only comments and builds
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
First recorded: 2026-08-22 (8b9cd77b94e, by 'internal-provenance-ships')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: on cas-blob-intents the `B<n>` tags are mostly gone: a grep of `[Bnnn]`/`(Bnnn)` forms finds 7 files in src/programs (e.g. src/IO/S3Defines.h, src/IO/S3/PocoHTTPClient.h, src/IO/S3/tests/gtest_aws_s3_client.cpp, gtest_cas_protocol_scenarios.cpp), against ~72 in the task; ProfileEvents.cpp and ThreadStatus.h no longer carry them; `M-W` still at gtest_ca_wiring.cpp:31. Changes: description: Update paragraph appended. Pointers stale: ProfileEvents.cpp:789 and ThreadStatus.h:91 no longer match; counts (239 lines, 72 files) are out of date. Would settle it: full list of remaining plan/backlog tags (the grep covered only bracketed B-forms and one M-W check).
---
<!-- COMMENTS:END -->
