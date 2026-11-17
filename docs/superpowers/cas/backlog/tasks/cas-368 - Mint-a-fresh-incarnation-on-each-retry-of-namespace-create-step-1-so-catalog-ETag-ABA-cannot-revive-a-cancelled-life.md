---
id: CAS-368
title: >-
  Mint a fresh incarnation on each retry of namespace-create step 1 so catalog
  ETag ABA cannot revive a cancelled life
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:ref-ledger'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:medium'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasNamespaceJanitor.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/catalog.md
priority: medium
type: bug
ordinal: 462000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`ref_catalog` has no revision field, so add-then-cancel restores identical bytes and the same content ETag. `writeLoop` resends the same bytes with the same `If-Match` (and a proxy can duplicate a PUT), so a late attempt can bring back a cancelled `Creating` life. The janitor then deletes only objects it listed before the rebirth whose ETag is unchanged — in practice the reborn life's `_ckpt`, leaving the table unrecoverable (`CORRUPTED_DATA`). No user data loss; GCS is immune. Snapshot/`_ckpt` SK9 depends on this.

Fix (no format change): mint a fresh incarnation on every retry of `createNamespaceStep1` (~40 lines). Does not cover a proxy duplicating one request; a catalog revision field is the durable fix (next format). `CaRefCatalogCore` models the token as a flag and cannot express ABA. Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/catalog.md (T5), docs/superpowers/cas/2026-10-12-late-request-audit/objects/snapckpt.md (SK9). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 retried step 1 never re-sends identical catalog bytes (gtest)
- [ ] #2 model tracks the catalog value and a late-apply action
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
2026-10-18: draft prepared on fix/antalya-26.6/cas-368-fresh-incarnation-per-step1-retry @d42febbcc9bf (base 8fefc27bd625, umbrella-reviewed, gated). Local, not pushed; waits for the owner's read. Report, review and body tmp/noclock/drafts/prep/368/.

2026-11-11: CAS-449 is a second scenario for the same change: a lost `PUT` response plus complete, drop and GC erase inside the retry window re-inserts an erased incarnation. The draft @d42febbcc9bf covers it (mint inside `mutate`, entry taken from the landed catalog). Target is now `antalya-26.8` after PR 2805 merges; the diff passes `git apply --check --3way` on 2f89c5db182 and needs a rebuild and the gates there. Owner decision 2026-11-11: no port to `antalya-26.6`. Planned delivery: one commit series in the same pull request as CAS-29.8.

Implemented 2026-11-11 on branch `fix/antalya-26.8/cas-ref-cleanup-per-life-licence` at `be74be9fa7b` (worktree `cas-pr1`, 39 commits over `2f89c5db182`). Not pushed, pull request not opened. Design: `docs/superpowers/specs/2026-11-11-cas-pr1-ref-cleanup-bundle-design.md`. Measurements: `docs/superpowers/reports/2026-11-11-cas-pr1-ref-cleanup-bundle-measurements.md`. Story A: `182c49c3c4e`, `99509ff68df`, `baaaac8ec8d`. Acceptance #2 (model with a catalog value and a late apply) stays open.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Delivered in PR 2831 by `531d7c293e0`. Open: the model of the catalog value and a late apply (AC2); CAS-456 holds the late-first-delivery premise.
<!-- SECTION:NOTES:END -->
