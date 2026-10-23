---
id: CAS-417
title: >-
  A Move whose acknowledgement and verifying read were both lost leaves the
  final-name ref behind a failed rename
status: To Do
assignee: []
created_date: '2026-10-22 23:27'
labels:
  - 'area:write-path'
  - 'area:ref-ledger'
  - 'complexity:medium'
  - 'risk:medium'
  - 'confidence:solid'
  - 'origin:review'
milestone: m-8
dependencies:
  - CAS-346
priority: medium
type: bug
ordinal: 520000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`moveRefExact` appends the Move through `appendRefOps`. The ledger already does a resolve-before-reissue read inside the attempt, so a landed write that is readable is reported as success; only when the PUT acknowledgement AND that read are lost does the lane wedge and the call throw write-retry-later (`NETWORK_ERROR`) although the record is durable. `MergeTree` then treats the rename as failed: the in-memory part keeps `tmp_*`, rollback renames `tmp_*` to `delete_tmp_*` (the source is gone, `moveDirectory` returns silently), and after a restart the final-name ref loads as a real part (replicated: detached as unexpected) — rows of an INSERT that reported an error appear.

A completion check inside `moveRefExact` (mirroring `republishRef`'s NETWORK_ERROR branch) does not help: `resolveRef` is an in-memory read of a lane that is still wedged, so it sees the pre-Move state; the wedge is settled only by the next append (`resolveWedgeOnce`). Candidate designs: (a) the one-shot disk transaction that performed the rename records `{dst, expected}` and its destructor runs `dropRefIfMatches(dst, expected)` best-effort once the lane recovers; (b) a ledger-level settlement call a caller may invoke after an ambiguous append; (c) MergeTree-side reconciliation of an unexpected final-name ref against the part log. Needs a fault test with `LatchedChunkFaultBackend` (`Mode::LandedThenLost`) at the transaction level — today only `CASPartFolderAccess.MoveRetryAfterAmbiguousAppendIsIdempotent` covers the facade. Source: third umbrella review of `feature/cas-disk-transactions` (2026-10-23, major 2).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Fault test: a Move whose acknowledgement and verifying read are lost, followed by the MergeTree rollback path, leaves no committed ref under the final name once the lane recovers
- [ ] #2 The chosen compensation is documented next to `moveRefExact` and in `architecture/replication.md`
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
