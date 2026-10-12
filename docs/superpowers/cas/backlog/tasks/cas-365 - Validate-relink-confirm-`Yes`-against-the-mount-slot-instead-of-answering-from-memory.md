---
id: CAS-365
title: >-
  Validate relink-confirm `Yes` against the mount slot instead of answering from
  memory
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-10-12 23:51'
labels:
  - 'area:replication'
  - 'area:mounts'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:medium'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/protocol.md
priority: medium
type: bug
ordinal: 459000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`CasRefLedger::confirmExactRef` answers `Yes` from in-memory state under `mayMutate`. A late `Yes` alone is harmless (the receiver's `+1` is durable first; `settleEntry` spares `indeg > 0`), but a sender that falsely believes it is the legitimate writer — frozen VM clock (C5), a same-pair twin (C6, see the epoch-monotone task), an epoch reuse (C9) — lets the receiver commit a dangling ref. The next-slot `404` probe is refuted (seals get reclaimed).

Fix (no format change): before `Yes`, `GET` the mount slot and require an ETag this renewer wrote with our `(uuid, epoch)` (~100 lines; single-flight, only on the path to `Yes`). Every foreign actor changes the slot first, so this is clock-free. Alternative: answer after the next successful renew (≤ P latency). `CaRelinkConfirmCore` needs `slotHolder`/`slotEtag` separate from `sFence`, a `ForeignClaim`, and a `SlotRead` in `Gate1Answer`. Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/protocol.md (C1–C10). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 model extension fails without the slot read and holds with it
- [ ] #2 relink-confirm refuses after a foreign claim even when the local fence still says live (gtest)
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
Same-pair twin fix: CAS-364.

2026-10-13 model (`7096317fc6c`, CaRelinkConfirmCore RESULTS `#cas-365`): slot holder separated from `sFence`, confirm and renewal split into steps. `cur_staleyes` (frozen clock, C5) violates `ConfirmedRelinkNeverDangles` in 13 states; `cur_twin` (C6) too but only before #2512 — on the release line claimMount refuses the same pair, so C6 is unreachable; C9 belongs to CAS-362/363. Fix A (one `GET` of the mount slot on the path to `Yes`, require our (uuid, epoch) and ETag = `last_etag` or the in-flight renewal's `write_attempt_id`, else `Unknown`) green 15.9 M; fix B (answer after the next renewal) green 26.7 M; sabotages red (reused pre-snapshot GET; compare only (uuid, epoch); renewal started before the snapshot). Recommendation A: ~100 lines in `Pool::confirmExactRef` + `slotStillOurs` on runtime/renewer, one GET per Yes, no format change, no clock; breaks the 'zero object-store I/O' contract (update comments in `CasRefLedger::confirmExactRef`, `ContentAddressedExchange.h`). Open: concurrency cap vs single-flight; exposing the in-flight attempt id; which plane admits the GET.
<!-- SECTION:NOTES:END -->
