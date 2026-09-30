---
id: CAS-328
title: >-
  Make every CAS on-store object Tolerant to unknown keys; decision-changing
  fields use the `!` prefix
status: To Do
assignee: []
created_date: '2026-09-30 18:02'
updated_date: '2026-09-30 18:02'
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
- [ ] #1 The four readers use `KeyStrictness::Tolerant`; tests that asserted `CORRUPTED_DATA` on an unknown ordinary key now assert the key is skipped
- [ ] #2 A `!`-prefixed unknown key still fails with `UNKNOWN_FORMAT_VERSION` in every reader (test per object)
- [ ] #3 The format spec states the rule: optional field = plain key, decision-changing field = `!` key; Strict is not used
- [ ] #4 Own PR to antalya-26.6, reader-only change, no writer change, no format version bump
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
