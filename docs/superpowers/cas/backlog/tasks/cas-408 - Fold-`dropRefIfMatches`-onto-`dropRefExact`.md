---
id: CAS-408
title: Fold `dropRefIfMatches` onto `dropRefExact`
status: Done
assignee: []
created_date: '2026-10-22 06:09'
updated_date: '2026-11-06 23:12'
labels:
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-4
dependencies:
  - CAS-346
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.cpp
priority: low
type: chore
ordinal: 511000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CachedPartFolderAccess::dropRefIfMatches` (rollback drop, `noexcept`, returns bool) and `dropRefExact` (foreground drop, throws, returns Removed/Absent/Mismatch) have the same body: one `appendRefOps` closure that removes the committed binding only while it still equals `expected`, a `RefDrop` audit event, `eraseView`. `dropRefExact` was added by the owner-Move series (branch `feature/cas-disk-transactions` on PR Altinity/ClickHouse#2579, CAS-346) and the older helper was left as is. Make `dropRefIfMatches` a `try { return dropRefExact(key, expected) == Removed; } catch (...) { log + CASRefRollbackBestEffortDropFailed; return false; }` wrapper; the audit reason text becomes neutral (today it names the rename). About -30 lines, one protocol closure instead of two. Callers: `ContentAddressedTransaction::commit` rollback (IfMatches), `publishStaging` scratch path and `republishRef` (Exact).

Provenance: umbrella review of the owner-Move series, 2026-10-22, Ockham perspective (simplification plan item 1); verified against `feature/cas-disk-transactions` @ 7a16106160c, `Parts/PartFolderAccess.cpp:773-884`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 `dropRefIfMatches` contains no `appendRefOps` closure of its own; it delegates to `dropRefExact` and keeps its noexcept contract (swallow, log, counter, return false)
- [x] #2 Existing proofs stay green: `CASPartFolderAccess.*DropRef*`, `gtest_ca_wiring`, `gtest_cas_parallel_commit`, stateless 05054 and 05386-05392
- [x] #3 dropRefExact returns `Cas::Removal` (`Backend/CasWriteResult.h`), not a private `DropRefExactOutcome {Removed, Absent, Mismatch}` duplicate
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
2026-10-22 consistency review (finding 2): `DropRefExactOutcome` near-duplicates `Cas::Removal {Removed, Gone, Mismatch}` returned by `deleteMetaExact` for the same "exact removal, tell absence from a raced replacement" purpose. Fold onto `Cas::Removal` while folding the two drop helpers.

2026-11-07 audit: merged in https://github.com/Altinity/ClickHouse/pull/2647 as d8d64d936e2 (antalya-26.8, 2026-10-27).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Done on `feature/cas-disk-transactions` (commit after 8f778ccfe8a, 2026-10-22): `dropRefIfMatches` = `try { dropRefExact(k, e) == Removal::Removed } catch { log + CASRefRollbackBestEffortDropFailed; false }`; `dropRefExact` returns `Cas::Removal` (`Gone` for an absent ref); audit reason neutral. Proofs: `CAS*` gate 2699/2699, stateless 05054, 05386–05392, 05004, 04292 green on the stand.
<!-- SECTION:FINAL_SUMMARY:END -->
