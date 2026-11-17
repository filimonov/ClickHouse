---
id: CAS-449
title: >-
  Namespace create reuses one incarnation across admission retries: an erased
  incarnation can be inserted again
status: Done
assignee: []
created_date: '2026-11-11 07:51'
updated_date: '2026-11-17 20:01'
labels:
  - 'area:ref-catalog'
  - 'area:gc'
  - 'origin:review'
dependencies: []
references:
  - docs/superpowers/specs/2026-11-11-cas-ref-cleanup-per-life-licence-design.md
  - docs/superpowers/models/CaRefCleanupLicenceCore_RESULTS.md
priority: medium
type: bug
ordinal: 552000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CasRefCatalog::createNamespace` (`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.cpp`) calls `mintFreshIncarnation` once, before the admission loop. `createNamespaceStep1` captures that entry by reference and inserts it on every retry of `casUpdateImpl` that finds the name absent. Path to reuse: the first `PUT` lands and its response is lost; the resolution read sees a different catalog body (another rewrite landed), so `CasRequests.cpp` returns `Conflict{any_ambiguous}` and `casUpdateImpl` pauses `Retry::backoff` (at most 5 s) and loops. Meanwhile a sibling opener of the same mount resumes the `Creating` row with `completeCreation` (own live fence, `CasRefLedger.cpp`), the table is dropped (`beginRemoving`), a fold records `cleanup_evidence`, and `CatalogLifecycleReconciler` erases the row. The retry then finds the name absent and inserts the same incarnation. Consequences: physical keys `cas/ns/stream/<incarnation>/...` and the GC `ref_lives` row are keyed by the incarnation, so a second life shares them with the erased one. `CaRefCleanupLicenceCore` (`_sab_reuseincarnation`, `_old_reuseincarnation`) shows covered-log cleanup deletes a live log of the successor when an incarnation is reused, with and without the whole-catalog etag condition. Found by source review at `3fa3a01203b`; the same code is on `antalya-26.6`. Not reproduced at runtime: the erase needs at least one committed GC round after the drop, the retry window is one request timeout plus the backoff pauses, and no happens-before excludes the overlap. Fix direction: mint the incarnation inside the admission's `mutate` (one per attempt that inserts) and take the entry for `completeCreation` from the catalog the admission returns.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest drives the interleaving through the step-1 hooks: lost response on the first PUT, sibling completion, drop, erase, retry; before the fix the second life carries the erased incarnation
- [ ] #2 After the fix no retry of one `createNamespace` call can insert an incarnation that an earlier attempt of the same call already wrote
- [ ] #3 `completeCreation` operates on the entry the successful admission wrote
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
2026-11-11 owner decision: no port of this fix to `antalya-26.6` (narrow window, complex scenario).
2026-11-11 overlap found: CAS-368 asks for the same code change from another scenario (catalog ETag ABA after a cancelled `Creating` row). Its draft `fix/antalya-26.6/cas-368-fresh-incarnation-per-step1-retry` @d42febbcc9bf (2 commits on 8fefc27bd625, +262/-17, local, not pushed) mints the incarnation inside `mutate` of `createNamespaceStep1` and returns the entry from the landed catalog, which is this task's fix direction and criteria #2 and #3. `git apply --check --3way` of that diff passes on the PR 2805 tree (2f89c5db182); it was built and gated only on the 26.6 base on 2026-10-18. Left for this task: criterion #1, the gtest of the lost-response, complete, drop, erase, retry interleaving. This task was filed without a backlog search for `createNamespaceStep1`; CAS-368 should carry the code, this task the second scenario and its test.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Delivered in PR 2831 by `531d7c293e0` with `f81f7f8f544`. Same change as CAS-368.
<!-- SECTION:NOTES:END -->
