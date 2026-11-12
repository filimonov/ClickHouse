---
id: CAS-455
title: >-
  Report whether a catalog write was sent, so an unsent `beginRemoving` reopens
  the write lane
status: To Do
assignee: []
created_date: '2026-11-11 16:16'
updated_date: '2026-11-12 01:08'
labels:
  - 'area:ref-ledger'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:late-request-audit'
dependencies: []
priority: low
type: feature
ordinal: 559000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`casUpdateImpl` (`CA/Pool/CasRefCatalog.cpp`) does not tell its caller whether any attempt reached the wire. `dropNamespaceImpl` therefore treats every failure after it entered `beginRemoving` as possibly sent and keeps the namespace's write lane closed until a retried removal completes or the disk is remounted. A failure before the first send (lease gate refusal, catalog read failure inside the update loop) closes the lane without need.
Fail-closed today; the cost is a table that refuses writes until the `DROP` is retried.
Provenance: tmp/noclock/drafts/prep/367/REVIEW.md (declined item 1).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A `beginRemoving` that fails before any write is sent reopens the lane; one that sent a write keeps it closed (gtest for both)
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
2026-11-12: implemented on branch `fix/antalya-26.8/cas-late-request-audit-bundle` (head `fa1b89d1529`, not pushed, no pull request yet), so this task ships with CAS-367 instead of after it. `CasRefLedger::dropNamespaceImpl` sets its lane-closing local from `RequestObserver::on_write_attempt` of the removal operation; the engine calls it before each write attempt. Commits: `e83bf616613` (close the lane only after a sent write), `85cd6215887` (a later proof of `Removing` clears `removal_write_may_land`), `5e063710e2a` (refusal text of `namespaceLife`), `fa1b89d1529` (test for `FencedOut` after a sent write). Acceptance criterion 1, suite `CASRefWriterNamespaceRemoval` in `src/Disks/tests/gtest_cas_ref_writer.cpp`. Nothing sent, lane reopens: `RefusedBeginRemovingBeforeAnySendReopensTheLane`, `EntryChangedBeforeAnySendReopensTheLane`, `ReadFailureInsideBeginRemovingReopensTheLane`. Write sent, lane stays closed: `LeaseGoneAfterASentAttemptKeepsTheLaneClosed`, `EntryChangedAfterASentAttemptKeepsTheLaneClosed`, `FencedOutAfterASentAttemptKeepsTheLaneClosed`. Mutation checks: setting the local before the call fails the three reopen tests; clearing it on `FencedOut` fails only the `FencedOut` test; clearing it on `EntryChanged` fails the `EntryChanged` test. Spec: `docs/superpowers/specs/2026-11-11-cas-pr2-late-request-audit-bundle-design.md` section 5.2. Close this task when the branch merges.
<!-- SECTION:NOTES:END -->
