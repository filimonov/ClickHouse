---
id: CAS-241
title: >-
  Write the CAS release notes and changelog entry, including the three
  user-visible upstream side effects
status: To Do
assignee: []
created_date: '2026-08-04'
updated_date: '2026-11-21 08:20'
labels:
  - 'area:docs'
  - 'area:upstream'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-0
dependencies: []
references:
  - programs/server/config.xml
  - docs/superpowers/cas/fable-review-triage.md
  - CHANGELOG.md
priority: medium
type: docs
ordinal: 306000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
No changelog line exists for CAS: `CHANGELOG.md` and `docs/changelogs/` have zero hits for content-addressed or antalya. `CHANGELOG.md`
is assembled from PR "Changelog entry" fields, so the fix is a "New Feature" entry in the CAS PR description plus the Antalya
release notes, not a hand edit. Behavior changes an upgrading user can hit and must be told about:
- `clickhouse-disks --query` now exits with the failing command's code (carve-out is CAS-61; no changelog line yet).
- the `lazy_load_tables` `SYSTEM`-command behavior change (written up nowhere).
- `cas_log` and `cas_gc_log` ship enabled by default (`programs/server/config.xml:1201,1321`).
- unknown CAS config keys are now rejected (`73f49694b37`).
- on `gcs_hmac`, every request is GOOG4-signed and `x-amz-*` headers are handled strictly (`fable-review-triage.md` M-item, point 2).

Provenance: BACKLOG/docs-and-cleanup.md#cas-changelog-entry-missing (from final-checks-todo.md item 12b) and [CHANGELOG-unknown-config-key-rejection]. Verified 2026-09-26 against 6eb16e1cc56.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The CAS PR description has a 'New Feature' changelog entry
- [ ] #2 The Antalya release notes list all five behavior changes above, each with the setting or command it concerns
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
First recorded: 2026-08-04 (0f266066bef, by 'CHANGELOG-unknown-config-key-rejection')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 CHANGELOG.md has 0 hits for content-addressed/antalya; cas_log config exists at programs/server/config.xml:1229. Changes: none. Pointers stale: config.xml cas_log now at :1229 (task says :1201). Would settle it: whether the CAS PR description has a changelog entry and Antalya release notes exist outside the repo.
---
<!-- COMMENTS:END -->
