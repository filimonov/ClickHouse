---
id: CAS-29.5
title: >-
  Discard condemn-HEAD hints the merge has passed so a mass-removal round stops
  paying inline HEADs (spec A2)
status: To Do
assignee: []
created_date: '2026-09-26 07:17'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
  - 'origin:soak'
  - 'origin:canary'
milestone: m-1
dependencies: []
references:
  - CA/Gc/CasGc.cpp
  - CA/Gc/CasGcReadAhead.h
documentation:
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#a2-pinned-head-window
  - docs/superpowers/cas/2026-09-04-gcs-soak-15min.md
parent_task_id: CAS-29
priority: high
type: bug
ordinal: 111000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`topUpHeadHints` (`CA/Gc/CasGc.cpp:1825-1830`) hints only while `reads.pending() < reads.window()`. `head_candidates` is a
superset in the merge's key order, so hints for keys the merge already passed are never taken, pin the window, and every
later `takeHead` (`:1852`) degrades to a serial HEAD. AWS soak 2026-09-04: misses 862 = inline HEADs 860, `Wasted` exactly
64 (the window), `fold_reduce` 128 s for 1,261 condemns versus 42 s for 1,793 with a free window. GCS no-chaos soak shows
the same signature with `epoch_crossings = 0`, so a second pinning mechanism may exist. On otel.demo the effect is small
(Wasted 0.5%, audit F24).
Fix: before topping up, and on any miss with `pending() == window()`, `discardHead` every pending hint whose key sorts before
the key being taken (counted as `CASGCReadAheadWasted`), then top up.

Provenance: BACKLOG/gc.md#gc-condemn-head-read-ahead-pinned-window (task part and GCS confirmation; the recovery-walk sibling is gc-recovery-walk-hint-guard). Verified 2026-09-26 against 59494ebf366 and 0dbbd797792 (no discard rule on either).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest with a gapped candidate superset matches hits, misses and wasted per round against the sequential oracle
- [ ] #2 On a mass-removal round `CASGCReadAheadMiss` is within one window of zero
- [ ] #3 A local reproduction of the GCS pinning without an epoch crossing either shows it is the same mechanism or names the second one
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
