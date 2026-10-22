---
id: CAS-409
title: Make `repointRef`'s expected manifest a required parameter
status: To Do
assignee: []
created_date: '2026-10-22 06:09'
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
- [ ] #1 `repointRef` takes a required expected manifest, publishes only with `PublishPolicy::replaceIfCurrent`, and has no unresolved-key branch
- [ ] #2 The byte-equal no-op path still refuses with `CAS_OWNERSHIP_CONFLICT` when the committed manifest differs from the expected one (gtest)
- [ ] #3 Existing proofs stay green: `CASPublishPolicy.*`, `CASPartFolderAccess.Repoint*`, stateless 05054
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
