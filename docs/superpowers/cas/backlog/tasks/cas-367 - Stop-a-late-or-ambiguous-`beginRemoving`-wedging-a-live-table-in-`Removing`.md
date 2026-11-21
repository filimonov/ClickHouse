---
id: CAS-367
title: Stop a late or ambiguous `beginRemoving` wedging a live table in `Removing`
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:ref-ledger'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:medium'
  - 'needs:decision'
  - 'origin:noclock-audit'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
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

Update 2026-11-21: Remaining scope: AC2 (stuck-`Removing` detector) and AC3 (decision on the catalog revision / `removal_by` format change). AC1 (lane stays closed after an ambiguous `beginRemoving`) landed in `7d814447cb1` and `c821aece830`, both in the branch.
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-18: draft prepared on fix/antalya-26.6/cas-367-keep-lane-closed-on-ambiguous-removing @c7a242d05bf8 (base 8fefc27bd625, umbrella-reviewed, gated). Local, not pushed; waits for the owner's read. Report, review and body tmp/noclock/drafts/prep/367/.

Implemented 2026-11-12 on branch `fix/antalya-26.8/cas-late-request-audit-bundle` at `fa1b89d1529` (worktree `cas-pr1`, 40 commits over `be74be9fa7b`). Not pushed, pull request not opened. Gates on that head: release `CAS*` 3005, ASan/UBSan 3009, Fast-test compile, stateless 58 of 58. Design: `docs/superpowers/specs/2026-11-11-cas-pr2-late-request-audit-bundle-design.md`. Measurements: `docs/superpowers/reports/2026-11-11-cas-pr2-late-request-audit-bundle-measurements.md`. Story C: `e06224820c0`; review fixes `e83bf616613`, `1c220158aaf`, `85cd6215887`, `fa1b89d1529`. The lane closes only after a removal write was sent.

CAS-487 does not deliver this task: Part B was split off. When Part B lands, nothing reopens a lane after a durable terminal record, and the stuck-`Removing` detector (AC #2) is replaced by the GC completer. The code of this task is rewritten by Part B.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. Lane closing delivered in PR 2831 by `7d814447cb1`, extended by `c821aece830` (a `Removing` row read by anyone closes the lane for good). Open: a stuck-`Removing` detector, and a written decision on the format change. The accepted residual is in `d0e2d1a80bd`.

2026-11-17: PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) keeps `removal_write_may_land` and adds `removing_observed` (c821aece830). A `beginRemoving` that lands after a remount leaves the row `Removing` under an open lane until a restart, a remount, a retried `DROP` or the GC leader's next round (accepted residual, d0e2d1a80bd). fc82bf0d80b latches a `Removing` row at the recovery confirmation.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: 7d814447cb1 and c821aece830 are ancestors of the checked branch (lane closing delivered, AC1); open per the 2026-11-17 note: the stuck-Removing detector (AC2) and the written format decision (AC3). Changes: description: Update paragraph appended. Size: S. Owner question: Is the catalog revision plus removal_by format change wanted, or is the accepted residual enough?
---
<!-- COMMENTS:END -->
