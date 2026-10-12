---
id: CAS-305
title: >-
  Bound the cost of sealing every skipped writer epoch when a long-idle table is
  first touched
status: To Do
assignee: []
created_date: '2026-09-26 12:54'
updated_date: '2026-10-12 12:37'
labels:
  - 'area:ref-ledger'
  - 'complexity:medium'
  - 'risk:medium'
  - 'touches:protocol'
  - 'confidence:solid'
  - 'needs:measurement'
  - 'origin:2031-triage'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp
  - 'https://github.com/Altinity/ClickHouse/issues/2031'
documentation:
  - docs/superpowers/cas/2031-triage.md
priority: medium
type: enhancement
ordinal: 384000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Writer recovery closes every dead epoch between the namespace's last seal and the live epoch with its own durable step. Each step is a conditional create of the seal at `{E, T+1}` (`op.create`, `R/Pool/CasRefLedger.cpp:1154`), then `publish_recovered_frontier` (`_ckpt` CAS with an exact read, `:1230`), counted by `CASRefRecoveryEpochSealed`. The walk is one loop (`:950-1230`) with no upper bound.
`writer_epoch` is per server root and is minted on every mount and on fence (`allocateWriterEpoch`), while the seal chain is per namespace. The first touch of a table idle across N remounts therefore costs O(N) sequential write pairs before the table is usable.
Per-epoch slot occupancy is forced by INV-1/INV-2, so the ask is to cut the cost per step or the number of steps, never to drop the seal.
Directions from the 2026-08-21 triage: (a) fold the `_ckpt` contributions of consecutive seals where the one-unfrontiered-transaction invariant allows, and skip the exact re-GET on intermediate epochs; (b) stop spending epochs where none is needed (overlaps DRAFT-29, `[fence-costs-epoch-distinct-mint]`); (c) log or count "N epochs sealed in one recovery" so the gap is visible before it costs minutes.
The ledger created this item as `{#recovery-seal-walk-per-skipped-epoch}` in `BACKLOG/ref-protocol.md` but never committed it, so no task carried it. Same code on antalya-26.6 (`CasRefLedger.cpp:1129`).

Provenance: docs/superpowers/cas/2031-triage.md#cas-114 (2031-triage CAS-114; the BACKLOG anchor #recovery-seal-walk-per-skipped-epoch it names was never committed, only 43b918b4ed6 mentions it); related DRAFT-29. Verified 2026-09-26 against cae9288ee65 (cas-gc-rebuild) and 8d62c314ec1 (altinity/antalya-26.6). Code reading plus protocol analysis, not observed, so capped at Medium.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A recovery that seals more than one epoch reports the number of epochs sealed and its wall time in a log line or `ProfileEvents`
- [ ] #2 A measurement records first-touch latency and request count for a table idle across 1, 10 and 100 remounts, on the current code and after the change
- [ ] #3 The chosen reduction (fewer `_ckpt` publishes, fewer exact reads, or fewer epochs minted) keeps INV-1/INV-2; the ref-ledger gtests and the TLA+ recovery model still pass
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
2026-10-12, direction (b) made precise. The epoch mint is one CAS per mount and is not the cost; the per-table seal is. A seal at `{E, T+1}` is needed only when a writer of epoch E may still have an in-flight conditional create for that table (INV-1: at most one per table and writer). A predecessor whose death is certified clean (farewell after every ref lane resolved its last attempt, which is what CAS-179 makes the farewell mean) has no such PUT in any table, so the successor may skip the seal walk for E in every table. The GC side already follows seal chains across epoch gaps (`crossEpochFromSeal` walks `prev_epoch_seal` back from the witness epoch); the writer loop in `runRecoveryWalkOnce` stops only at `epoch >= live_epoch` and seals every numeric epoch, so it needs a durable 'epochs ended clean' record on the server root (the farewell body is overwritten by the next claim) and the skip must be checked in the TLA model. A dirty death still needs one seal per table in that epoch, idle or not: the ghost's one in-flight PUT may be that table's `{E, 1}`. Removing the epoch from `RefTxnId` does not help: a single per-lane counter still needs the positional fence at T+1 on every first touch after an uncertified death, and loses the per-record incarnation tag the stale-precommit sweep (`mref.writer_epoch < live_epoch`) and GC's `(writer_epoch, build_seq)` order use.

2026-10-12, direction (d), never evaluated: one sequence per namespace with the same start seal. The July spec (git show 6fff5748abe:docs/superpowers/specs/2026-07-27-cas-ref-chain-complete-cut-design.md, INV-1/INV-2) makes ids dense within `(namespace, epoch)` and restarts at 1 per epoch, so a successor must close every elapsed epoch, empty ones included (sequence-1 seal grammar). With one counter per namespace the first touch seals the first absent slot T+1 once, and that fences every earlier incarnation: by INV-1 each had at most one in-flight create, always at its own first absent slot, which is either filled since or equals T+1. Cost: a `RefTxnId` format bump (keys, `_ckpt`, `_snap`, fold-seal coverage; frozen since 26.6.4, so a new version with a compatibility path), the seal carries the closed `writer_epoch` for the sweep rule 'manifest of epoch E deletable once a seal above E is consumed', `crossEpochFromSeal` and the hinter's epoch bound disappear, TLA re-check. Gain is O(1) seals per first touch instead of O(elapsed epochs), and no epoch crossings in GC; it does not cut seals for tables loaded at every restart (one touch per restart either way), so it does not replace CAS-358. The spec's alternatives table has no row for this variant; it was not rejected, only not considered.
<!-- SECTION:NOTES:END -->
