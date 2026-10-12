---
id: CAS-375
title: Audit and split TLA+ actions that fuse steps the code performs separately
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-10-12 11:52'
labels:
  - 'area:testing'
  - 'complexity:large'
  - 'confidence:solid'
  - 'risk:medium'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/SUMMARY-w1.md
  - docs/superpowers/models
priority: high
type: task
ordinal: 469000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Two data-loss defects survived model checking because one action fused separate code steps (`CaGcCondemnMarkerGate:WAdopt` fuses `HEAD` and marker `GET`; `CaMountObserveCore:Write` fuses admission and store landing). A codex audit of model atomicity vs code found 11 such issues in 14 models (wave 1: docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/SUMMARY-w1.md; wave 2 pending for the remaining 21 models).

Wave 1 fixes to the models, failing-first: split `WAdopt`/`ObserveMeta`/`Adopt` into body read and marker read; orphan sweep deciding from an earlier snapshot and a writer staging before namespace creation (`CaGcRootLocalPartManifestCore`); `SdkResend` in blob models; split `Write` into `WriteAdmit`/`WriteLand` with `MaxFlight`; response vs landing states in `CaMountRenewRetryCore`; let `CaRefDeltaIntakeCore` allow a losing round's delete to land; deletable seals and epochs in `CaRefTableSnapshotLogCore`; catalog value and late apply in `CaRefCatalogCore`; slot holder separate from fence in `CaRelinkConfirmCore`. Review rule to add: every action that reads or writes store state must list the code requests it stands for.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 every UNSOUND fusion from both waves has a model split or a written soundness argument
- [ ] #2 review checklist for models gains the atomicity rule
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
Wave 2 done: docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/SUMMARY-w2.md. Model-fidelity items to add: `CaGcDestructiveGateCore` gates condemnation while code gates only graduation/deletion (and default/empty-proof differ); gc models erase abort/timeout intermediate states (a pre-CAS delete or artifact landed, round lost); `CaDiskLifecycle` fuses log durability with acknowledgment; `CaRelinkLaneComposition` restricts the deletion actor more than code; heartbeat create landing after timeout defeats the historical `GcLiveGate` proof; `CaGcRoundDeferCore` uses an atomic guard the code lacks. Code findings filed as separate tasks.
<!-- SECTION:NOTES:END -->
