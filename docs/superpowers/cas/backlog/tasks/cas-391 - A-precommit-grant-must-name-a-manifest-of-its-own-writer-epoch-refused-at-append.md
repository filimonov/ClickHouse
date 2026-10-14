---
id: CAS-391
title: >-
  A precommit grant must name a manifest of its own writer epoch, refused at
  append
status: To Do
assignee: []
created_date: '2026-10-14 17:25'
labels:
  - 'area:writer'
  - 'complexity:low'
  - 'risk:medium'
  - 'origin:review'
dependencies: []
priority: high
ordinal: 489000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
**A precommit grant must name a manifest of the transaction's own writer epoch; refuse it at append otherwise.** Found by the codex review of the CAS-358 spec (`tmp/cas358/codex_spec_review_6.md`, finding 2).

### Defect

`PartWriteTxn::precommitAdd` checks the live epoch in `requireAlive` (`Pool/CasPartWriteTxn.cpp:186`), then `appendRefOps` captures the ref-table runtime later (`Pool/CasRefLedger.cpp:2050`, `acquireMutableRefTableRuntime`). A self-remount between the two (`Pool/CasPool.cpp:1504`, epoch bump, recovery cancel, table quiesce) does not invalidate a call that has not captured a runtime yet. The append then mints an epoch-4 transaction whose `AddPrecommit` names an epoch-3 manifest. `applyOwnerTransition` (`Pool/CasRefProtocol.cpp:245`) checks owner uniqueness only.

Consequences: the orphan sweep's rule (1) (`manifestDeletionPremise`, "grants do not cross epochs, so a sealed cursor in a higher epoch proves every grant of the build is folded") and the mount-lease floor (`prefixEligibleUnder`: an older epoch is retired) both assume the invariant. Under the schedule above the sweep deletes an owned body whose `+1` is above the fold cursor. The TLA `AdmissionBound` assumption states the same contract at the model level.

### Fix

One comparison at append, in `appendRefOpsOnRuntime` or in the precommit closure: `new_binding.manifest_ref.epoch == runtime writer epoch` for `AddPrecommit`, else `throwCasWriteRetryLater` (the build restarts under the live epoch, as `requireAlive` already does). `Promote` and removals are exempt: they move or end an existing binding. Replay (`trusted` apply) is unchanged; a durable log that already violates this is detected by fsck, not refused at load.

### Related

- CAS-358 stage 2 depends on it (spec `docs/superpowers/specs/2026-10-14-cas-orphan-sweep-cadence-and-snapshot-view-design.md`, §4.2).
- CAS-36, CAS-362, CAS-363: the other prerequisites of the floor and epoch proofs.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 appendRefOpsOnRuntime (or the precommit closure) refuses an AddPrecommit whose manifest_ref.epoch differs from the runtime's writer epoch with retry-later; Promote and removals unaffected
- [ ] #2 gtest: precommitAdd paused after requireAlive (test hook), self-remount to the next epoch, resume: append refused, no AddPrecommit of the older epoch in the log, build restarts under the live epoch
- [ ] #3 gtest: trusted replay of a log containing such a grant still loads; fsck reports it
- [ ] #4 CaGcRootLocalPartManifestCore: AdmissionBound documented as enforced by this check, not assumed
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
