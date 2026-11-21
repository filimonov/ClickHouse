---
id: CAS-504
title: >-
  DATA LOSS: GC deletes a blob body that a no-bytes copy in another namespace
  still names (per-namespace cut skew)
status: To Do
assignee: []
created_date: '2026-11-19 11:06'
updated_date: '2026-11-21 08:23'
labels:
  - cas
  - gc
  - data-loss
  - 'area:gc'
  - 'complexity:large'
  - 'risk:high'
  - 'confidence:solid'
  - 'impact:data-loss'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: critical
type: bug
ordinal: 609000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Reproduced at `work/antalya-26.8/cas-blob-intents` tip `1fbdd9555c4` by a deterministic gtest (patch: `tmp/intents/reports/crossns-repro.patch`, appended to `gtest_cas_gc_intents.cpp`; cases `DestinationFirst` and `SourceFirst` lose the body: `deleted` 1, fsck `dangling` 1; the control `BeforeTheRound` keeps it). Schedule: W reuses body t inside the round that condemns it (the round commits `pending(t)`); inside the NEXT round's intake a no-bytes copy of W's reference is made into another namespace and W's part is dropped; the fold reads the destination namespace before the copy and the source after the drop: zero, G4 deletes t. Cause: the cut is per namespace: each is proven when its own next key reads absent (`CA/Gc/CasGc.cpp:2967-2987`); pass 1 reads each namespace once in name order; nothing re-probes before destructive decisions. Operations that copy and drop by themselves: `MOVE PARTITION TO TABLE`, `RENAME TABLE` across namespaces (`republishRef`); with a following synchronous drop: `ATTACH PARTITION FROM`, `REPLACE PARTITION`, replica fetch by relink. Window (estimated): 10-50 ms on a small pool, 1-2 s across 1000 namespaces, up to 60 s for a busy source namespace. Lost: one column file of a part, for every replica. Versions: the intent protocol deletes after two zero cuts and is exposed to a ONE-hop schedule; the marker protocol (base `0aa837e942e`, released 26.6) needs three zero cuts: the one-hop schedule is spared there, a two-hop relay would be needed (reasoning, not run). Fix options: (1) a second collect of positive edges after the intake (GC-only, no format change; one GET per namespace per round with destructive work); (2) no-bytes copies read the intent (2 requests per blob on relink / hardlink paths); (3) a hand-over record in the source log (format change); (4) restore the three-cut spacing (mitigation only). Report: `tmp/intents/reports/crossns-loss.md`. PR 2917 must not merge without a fix.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:23
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1: repro tests are committed (4e478a0ce9f, 2389c55c9f5; gtest_cas_gc_intents.cpp, gtest_cas_gc_round_interleave.cpp); no GC commit found that re-probes or collects positive edges before destructive decisions. Changes: area set to gc; size unset -> large (L): fix option 1 needs a second collect with tests and a model check; options 3 changes the format; risk set to high; confidence set to solid; impact:data-loss; type unset -> bug; priority high -> critical: reproduced data loss (a column file of a part, for every replica) through MOVE PARTITION TO TABLE and RENAME TABLE across namespaces: normal operations, narrow window. Size: L. Links: PR 2917 must not merge without a fix; CAS-505 (coverage gap that hid it). Would settle it: Run DestinationFirst / SourceFirst on the branch tip to see whether they still lose the body, and check whether a fix is staged in the spec/plan work on this topic.
---
<!-- COMMENTS:END -->
