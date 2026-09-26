---
id: CAS-32
title: >-
  Give operators a repair path for an undecodable `_ckpt` or ref-log body
  instead of recreating the pool
status: To Do
assignee: []
created_date: '2026-09-26 07:03'
labels:
  - 'area:fsck'
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:high'
  - 'touches:protocol'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:review'
milestone: m-8
dependencies: []
references:
  - CA/Gc/CasGc.cpp
  - CA/Pool/CasRefCkpt.h
  - CA/Tools/CasFsck.cpp
priority: medium
type: feature
ordinal: 39000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Today the only remedy is in the stuck-removal warning: "restore the exact object or recreate the pool"
(`stuckRemovalWarning`, `CA/Gc/CasGc.cpp:162-185`). `publishCkpt` fails on the same undecodable body, so the namespace
cannot heal itself, and with the pool-wide suppression (see the halted-reclamation task) the whole pool stops reclaiming.
Candidates: `SYSTEM CAS FSCK` / `cas-fsck --repair` that rebuilds the checkpoint from the stream, or a
recreate-on-undecodable arm in the writer. Protocol-adjacent: owner decision on which one.

Provenance: BACKLOG/gc.md#ckpt-damage-no-repair-path part (b); verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The owner picks the repair mechanism
- [ ] #2 A test corrupts one `_ckpt` and shows the chosen repair returns the namespace to normal folding without data loss
- [ ] #3 The runbook names the repair step instead of pool recreation
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
