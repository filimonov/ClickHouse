---
id: CAS-367
title: Stop a late or ambiguous `beginRemoving` wedging a live table in `Removing`
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:ref-ledger'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:medium'
  - 'needs:decision'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/catalog.md
priority: medium
type: bug
ordinal: 461000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`beginRemoving` (`Pool/CasRefCatalog.cpp`) decides `Live → Removing` by comparing the row by value; a takeover does not change the row, so a zombie's late `DROP` lands on the successor's live table (Z7). `dropNamespaceImpl` also reopens the write lane after an ambiguous `beginRemoving` when one read still shows `Live`, so the same late landing happens without a zombie (A13, verified branch). Result: writes stop at the next append and the table refuses to open after restart; there is no `Removing → Live`. No silent loss: GC clears the row only after the terminal record is folded.

Now: keep the lane closed after an ambiguous `beginRemoving` (~10 lines); detector for rows in `Removing` with no terminal record for N rounds. Next format: catalog revision plus `removal_by` on `Removing` rows (old readers reject loudly). Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/catalog.md (A-Z7, A13). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ambiguous `beginRemoving` never reopens the lane (gtest)
- [ ] #2 a stuck-`Removing` detector exists (event or fsck check)
- [ ] #3 decision recorded on the format change
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
