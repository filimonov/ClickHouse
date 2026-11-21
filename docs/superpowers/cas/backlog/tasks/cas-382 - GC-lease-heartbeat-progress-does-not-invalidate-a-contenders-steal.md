---
id: CAS-382
title: 'GC lease: heartbeat progress does not invalidate a contender''s steal'
status: To Do
assignee: []
created_date: '2026-10-12 11:52'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:low'
  - 'origin:noclock-audit'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
milestone: m-8
dependencies: []
references:
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-gclease.md
priority: medium
type: bug
ordinal: 476000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A contender reads a frozen heartbeat, the incumbent pulses, the contender's `gc/state` CAS still succeeds: `NoFalseSteal` fails. Liveness only: the state-token fence still protects the displaced leader's publication. `CaGcLeaseCore`'s `hbAdvanced` rule is stricter than the code. docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-gclease.md (finding 3). Found by the codex model-atomicity audit (wave 2) on 2026-10-12; code steps verified by codex, harm as marked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 either condition the steal on the observed heartbeat token or relax the model rule and document it
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
2026-11-11 defaults review, field evidence: with `cas_gc_interval_sec` 10 (steal window 2 ticks = 20 s) under 8 INSERT/s the follower stole the GC lease while the healthy leader was 129 s into a round; the leader aborted at commit (`gc/state moved during the round`, Code 236) and the thief's own 135 s round aborted the same way: two rounds lost (run `gc_i10`, 04:24-04:27, master/tmp/cas297/runs/gc_i10). Not seen at 60 s with rounds up to 298 s (`gcH_i60_long`). When the leader is really cut off the steal is correct at 10, 30 and 60 s (`steal_gc*`). Besides the token condition of AC #1, consider a floor for the steal window that does not depend on the interval. Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (worktree ~/workspace/ClickHouse/cas-297-defaults); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 correction after an independent check of the report against run data (docs/superpowers/reports/2026-11-11-cas-defaults-review.check.md.txt, a49f8a8e45f): the note of 2026-11-11 said the leader was 129 s into its round at the steal. It was about 25 s in (leader round started 04:24:25, thief's round 04:24:50); 129 s is the leader's total duration when it aborted at commit. Thirteen earlier rounds of the same run were longer than the 20 s window and were not stolen, so the heartbeat protects the leader normally and failed once. One event in one run.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 Gc/CasGc.cpp:5494-5530 acquireOrRenewLease reads the gc heartbeat object inside the update callback, then CASes only gc/state; a pulse between the read and the CAS does not fail the steal. Model rule hbAdvanced at CaGcLeaseCore.tla:149 is stricter. Changes: impact:availability.
---
<!-- COMMENTS:END -->
