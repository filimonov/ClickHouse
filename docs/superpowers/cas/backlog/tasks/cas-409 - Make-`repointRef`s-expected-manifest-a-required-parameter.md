---
id: CAS-409
title: Make `repointRef`'s expected manifest a required parameter
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
ordinal: 512000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CachedPartFolderAccess::repointRef(key, entries, op, std::optional<ManifestRef> expected_current = nullopt)` keeps three branches for the unset case: `expected` derived from the fresh resolve, `PublishPolicy::replace()` instead of `replaceIfCurrent`, and the LOG_WARNING 'create-shaped call' branch. The only production caller (`ContentAddressedTransaction::publishStaging`) always passes `st.base_manifest_ref`: the repoint block runs only when the ref resolved, and 'resolved but no base' already threw `CAS_OWNERSHIP_CONFLICT` before it. So the fallback is reachable only from gtests (12 of 13 test calls). Make the parameter `const ManifestRef &`, always publish with `replaceIfCurrent(expected)`, keep the pre-check before the byte-equal no-op (that path does not go through `publishEntries`), delete the warning branch; the test calls resolve the ref before calling. About -25 lines plus mechanical test edits. Subsumes CAS-45: the unresolved branch no longer exists.

Provenance: umbrella review of the owner-Move series, 2026-10-22, Ockham perspective (simplification plan item 2); verified against `feature/cas-disk-transactions` @ 7a16106160c, `Parts/PartFolderAccess.cpp:656-719`, `ContentAddressedTransaction.cpp:391-468`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 `repointRef` takes a required expected manifest, publishes only with `PublishPolicy::replaceIfCurrent`, and has no unresolved-key branch
- [x] #2 The byte-equal no-op path still refuses with `CAS_OWNERSHIP_CONFLICT` when the committed manifest differs from the expected one (gtest)
- [x] #3 Existing proofs stay green: `CASPublishPolicy.*`, `CASPartFolderAccess.Repoint*`, stateless 05054
- [x] #4 repointRef takes a `PublishPolicy` (or the bare `ManifestRef` it needs), never an `std::optional<ManifestRef>` converted internally
- [x] #5 `moveRefIfMatches` is renamed `moveRefExact`: the `*Exact` suffix is the one that predicts "foreground, mismatch reported distinctly" (`deleteMetaExact`, `confirmExactRef`, `resolveExactRow`, `dropRefExact`); `dropRefIfMatches` keeps the noexcept rollback meaning
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
2026-10-22 consistency review (finding 3): the `IfMatches` suffix now carries two opposite contracts (`dropRefIfMatches` noexcept/swallowing vs `moveRefIfMatches` throwing `CAS_OWNERSHIP_CONFLICT`); the backend vocabulary is `create`/`replace(key, bytes, seen)` and `expected_value`, nothing uses `IfCurrent`. Rename in this task together with the signature change.

2026-11-07 audit: merged in https://github.com/Altinity/ClickHouse/pull/2647 as d8d64d936e2 (antalya-26.8, 2026-10-27).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Done on `feature/cas-disk-transactions` (same commit as CAS-408): `repointRef(key, entries, op, const ManifestRef & expected)` throws `CAS_OWNERSHIP_CONFLICT` unless the ref still names `expected`, publishes only `replaceIfCurrent(expected)`, no unresolved-key branch; `moveRefIfMatches` → `moveRefExact`; 12 gtest call sites pass the resolved manifest; `RepointWithoutExpected…` test renamed `ReplacePolicyRepointsWithoutAnExpectation` (it tests the `Replace` policy, which now has test callers only — dropping the mode is left to the `PublishPolicy` variant item). Proofs: `CAS*` 2699/2699, 05054, 05386–05392.
<!-- SECTION:FINAL_SUMMARY:END -->
