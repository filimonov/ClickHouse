---
id: CAS-384
title: >-
  Adopt a present body with an absent marker when its envelope is one of the
  writer's own attempts
status: To Do
assignee: []
created_date: '2026-10-12 15:28'
labels:
  - 'area:write-path'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:low'
  - 'needs:measurement'
  - 'origin:noclock-audit'
milestone: m-8
dependencies:
  - CAS-360
priority: low
type: enhancement
ordinal: 478000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After CAS-360 (fix C) a present body with an absent marker is always republished, so a publication that landed but lost its response costs one extra upload. A cheap refinement restores the old 'no reupload' behaviour safely: on an absent marker, read the envelope header (range GET of the first `blob_header_len` bytes) and adopt only if its `incarnation_tag` is one this `ensureBlobPresent` call minted. That body is the writer's own fresh publication, protected by its own precommit edge; a foreign or old incarnation is still republished. Low value: on otel.demo all absent-marker cases together were 8 in 17 days (`CASMetaAdoptBackfill`). Do it only if the republication rate shows up in measurements. See `docs/superpowers/specs/2026-10-12-cas-late-request-fixes-design.md#supersedes`.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
