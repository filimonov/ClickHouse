---
id: CAS-387
title: >-
  Drop the writer epoch from the ref-log sequence: one monotone sequence per
  namespace life
status: To Do
assignee: []
created_date: '2026-10-13 20:55'
labels:
  - 'area:ref-ledger'
  - 'touches:format'
  - 'touches:protocol'
  - 'complexity:high'
  - 'risk:high'
  - 'confidence:plausible'
  - 'needs:decision'
  - 'origin:review'
dependencies: []
priority: medium
ordinal: 482000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The ref-log key (cas/ns/stream/<life>/_log/<epoch-hex16>-<seq-hex16>.zst) carries the writer epoch as a second coordinate. The ghost-writer fence does not need it: the mount lease's fence deadline silences a superseded writer before the successor mints an epoch, and the positional seal (a write-once create at the next absent slot, INV-1 one in-flight per table) catches the one PUT that can still land. Both work unchanged on one monotone sequence per life: seal at T+1, a create conflict means the ghost's PUT landed, adopt it and seal at T+2.

What the epoch really provides is one fact: grants do not cross epochs, so every +1 of a build of writer epoch E lies between E's start and E's seal. The orphan sweep's rule (1) (CasOrphanManifestSweep.cpp manifestDeletionPremise) and the snapshot-based view (CAS-358) rest on it. Without the epoch in the key, the seal record carries the writer epoch it closes, and the coverage row (or _ckpt) carries the highest closed epoch per life; rule (1) becomes 'the seal closing E is at or below the cursor'. Builds, manifest keys and mount leases keep their writer epoch; only the log sequence drops it.

What goes away: one seal per skipped numeric epoch per table at first touch (CAS-305; 3,876 seal-only epochs on the PR 2542 pool), the prev_epoch_seal back-chain and crossEpochFromSeal in all four readers (writer recovery CasRefLedger.cpp:797, fold intake CasGc.cpp:2440, sweep, fsck), read-ahead overshoot past a seal (hintRefLogsWithinEpoch; 4.85M wasted GETs per week on otel.demo), the dense-epoch invariant and its hold grammar, the epoch-crossing hold shapes; the cursor becomes one integer and the hint bound min(T, C) has no exceptions. Table load after a long idle (one serial GET per seal-only epoch) shrinks to snapshot plus tail.

Transition options, on-store format frozen since 26.6.4: (a) format v2 for lives created after the upgrade, old lives read by the old walker until dropped, two walkers for the transition; (b) a pinned epoch constant in the existing key layout (keys unchanged, seal payload gains the closed writer epoch, writer recovery seals once at T+1 for such a life, cross logic inert) which bounds the diff but is still a record-format change; (c) rebirth of a life at first touch: the writer, having just recovered the table, publishes a v2 snapshot as the new genesis and the old chain becomes cleanup debris.

Not a fix for the orphan sweep's second read of the ref history (CAS-358): that is the view source, independent of the key layout. Sequence: CAS-358 on the snapshot rule first (no format change), this task after the format-version decision. Direction (d) of CAS-305 is this task; the single-sequence model is in CAS-305's notes (2026-10-12).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A design spec decides between transition (a), (b) and (c) with the compatibility path for 26.6.4 readers and the migration of live lives
- [ ] #2 The seal record carries the writer epoch it closes; the sweep's rule (1) and the CAS-358 snapshot rule are restated on it and proven in the TLA+ ref-chain model
- [ ] #3 A first touch of a life idle across N remounts writes one seal, not N; writer recovery and fold read no seal-only epochs
- [ ] #4 crossEpochFromSeal, prev_epoch_seal and the epoch-crossing hold reasons are removed from the new-format readers; read-ahead issues no request above committed_through
- [ ] #5 fsck, cas-inspect and the TLA+ models (ref-chain, fold clamp recovery, part-manifest core) follow the new key grammar
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
