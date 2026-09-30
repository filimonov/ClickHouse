---
id: CAS-328
title: >-
  Remove `KeyStrictness`: every CAS reader skips unknown keys; decision-changing
  fields use the `!` prefix
status: To Do
assignee: []
created_date: '2026-09-30 18:02'
updated_date: '2026-09-30 18:04'
labels:
  - cas
  - gc
  - format
dependencies: []
priority: medium
ordinal: 417000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Four readers are `KeyStrictness::Strict` and turn any unknown key into `CORRUPTED_DATA`: `cas_run` rows (`CasRecordStreamFormat.cpp:253`), fold seal (`CasFoldSealFormat.cpp`), `cas_ref_ckpt` (`CasRefCkptFormat.cpp:138`), ref catalog (`CasRefCatalogFormat.cpp`). Every other object is `Tolerant`. Strict readers make every optional field addition a compatibility-version bump (`G_BUILD`), which needs the not-yet-implemented write-down floor; the shipped 26.6.4 readers stay strict forever, so the payoff starts with the first release that carries this change.

The only stated motivation is the `_ckpt` comment: a field that changes what cleanup may delete must not be silently ignored. The evolution rule already covers that: such a field is written with the `!` prefix and an old reader fails with `UNKNOWN_FORMAT_VERSION`. Strict duplicates that rule as a guard against a writer forgetting `!`.

Owner decision 2026-09-30: all objects Tolerant unless a very important reason exists; none was found.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A `!`-prefixed unknown key still fails with `UNKNOWN_FORMAT_VERSION` in every reader (test per object)
- [ ] #2 The format spec states the rule: optional field = plain key, decision-changing field = `!` key; Strict is not used
- [ ] #3 Own PR to antalya-26.6, reader-only change, no writer change, no format version bump
- [ ] #4 The `KeyStrictness` enum, the traits-table column and the `JsonObjectReader` parameter are gone; `skipUnknown` keeps only the `!` branch (`UNKNOWN_FORMAT_VERSION`) and the skip
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
Owner decision 2026-09-30: drop the switch itself, not just flip the four Strict sites. The switch is declared twice (traits table in `CasFormat.cpp:103-129` and again at every reader call), and the two disagree for `cas_gc_maintenance_state` (Strict in the table, Tolerant at the reader), which nobody noticed because the table column is never read. 72 mentions in 15 files, all mechanical; the only behavioural change is the four Strict readers (`cas_run`, fold seal, `cas_ref_ckpt`, ref catalog) now skipping unknown ordinary keys.
<!-- SECTION:NOTES:END -->
