---
id: CAS-511
title: >-
  GC stops reclaiming for good when only stalled Creating catalog rows remain;
  such rows leak
status: To Do
assignee: []
created_date: '2026-11-21 03:33'
updated_date: '2026-11-21 08:24'
labels:
  - cas
  - gc
  - bug
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:high'
  - 'confidence:solid'
  - 'impact:cost'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: high
type: bug
ordinal: 616000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
An interrupted `CREATE TABLE` (a kill, or a pause past the mount lease, between writing the `cas/ref_catalog` row in state `Creating` and flipping it to `Live`) leaves a `Creating` row that nothing ever moves: a failed CREATE registers no table and the namespace contains the table UUID, so it is never reopened or dropped; GC skips `Creating` rows (`Gc/CasGc.cpp:296-305`), the pre-round catalog cleanup handles only `Removing` (`CatalogLifecycleReconciler.cpp:31`), there is no start-up scan and no licence.

Effects: (1) every such row leaks forever (catalog entry, admission reservation, checkpoint object); (2) when every table of the pool is dropped, the frontier has zero walkable namespaces and the catalog is not proved empty (`CasGc.cpp:2105` requires zero rows of any state), so GC suppresses every destructive step in every round, forever: soak of 2026-11-21 (`tmp/intents/soak-merge/runs/combined-12h-20261121`): 11,490 bodies and 11,502 intents never reclaimed, 75 rounds logged 'frontier INCOMPLETE (0 of 0 namespace(s) proven; catalog holds 3 row(s), none walkable/provable)'.

Older than PR 2917 (unchanged against 0aa837e942e). Visible only as a per-round Warning and `CASGCClampSuppressedPasses`; fsck says 'pending_gc ... expected, no action needed' and lists no catalog rows. Workaround: read the UUID from `cas/ref_catalog`, `CREATE TABLE ... UUID '<uuid>'` on the creator's server (reconciles to Live), then DROP.

Root-cause analysis: `tmp/intents/soak-merge/drain/RCA.md`. Mini-spec of the fix and its independent review: `tmp/intents/wave6/creating/SPEC.md`, `astra.out`: the GC leader cancels a `Creating` row whose creator fence is terminal by `isCreatorFenceTerminal` (a certificate: higher epoch, gc_fenced, farewell; never a clock; an absent slot is not terminal), with the existing `cancelStalledCreating`; fsck reports `stalled_creating`.

Also open from the same analysis: an older run's store held 5 `Live` catalog rows with no table: what leaves them is not established.

Update 2026-11-21: A first implementation exists at 90608d01636 (not pushed, not in PR 2917). Remaining scope is the review's required list: the creator cancels its own row at the failure site on a live server; memoize and cap the per-round slot probes; visibility (fsck class for every Creating row, per-state warning breakdown, INFO line and counter per cancel); fix the fsck pipeline note; missing tests; the smaller items. The description above is the problem statement, not the remaining work.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
## Umbrella review of the first implementation (2026-11-21)

Commit under review: `90608d01636` on `work/antalya-26.8/cas-blob-intents` (worktree `cas-pr1`), NOT pushed, NOT part of PR 2917. Review report: `tmp/intents/wave6/creating/umbrella/REPORT.md` (12 reviewers; per-reviewer outputs `NN-name.md`, the neutral context `CONTEXT.md`, the diff `diff.patch` in the same directory). Design: `tmp/intents/wave6/creating/SPEC.md`, its independent review `astra.out` and `ADDENDUM.md`. Root cause: `tmp/intents/soak-merge/drain/RCA.md`.

Verdict: request changes; no blocker. The predicate (a certificate, never a clock) and the exact compare-and-swap were not disputed; six distributed interleavings and four invariants were traced clean; no data-loss schedule.

Owner decision 2026-11-21: implement in a SEPARATE pull request (not in 2917); candidate for a stabilization PR of many small stories, one commit per story.

Required before that PR:
1. NOT COVERED: a `CREATE` that fails on a LIVE server (step 2 fails, no restart: e.g. '_ckpt ... gave up at the lease deadline') leaves the row under the creator's CURRENT epoch: no certificate (`classifyFenceCertificate`, `Pool/CasServerRoot.cpp:1575-1587`), `DROP` answers retry-later (`Pool/CasRefLedger.cpp:5696+`), a retried CREATE has a new UUID. Same for a server killed and never remounted. Fix: the creator cancels its own row at the failure site (it needs no certificate about itself).
2. Cost and liveness: one mount-slot GET per `Creating` row per round, not memoized per creator; uncancellable rows cost a GET every round forever; each cancel is a whole-catalog swap + re-read, no cap per drain; a store error on ONE creator's slot fails the whole pre-fold drain and the round. Memoize per (root, epoch) within a drain; cap or batch; pass the frozen policy; decide and document the probe-error policy; move the authority refresh out of the catalog hot-key lane.
3. Visibility: fsck counts only provably-terminal rows; rows with an absent / same-epoch / undecodable slot get no class; the GC warning prints 'N row(s), none walkable/provable' with no breakdown; a cancel writes no log line or event and is added to the drain phase's `deleted` metric. Report every `Creating` row with creator root, epoch and slot status; break the warning down by state; one INFO line and a separate counter per cancel; document the operator action for an unprovable row.
4. The fsck pipeline note says 'GC deletes none of them while every catalog row is Creating' whenever `stalled_creating > 0`: wrong on a pool with live tables (`Tools/CasFsck.cpp:1933-1936`).
5. Tests that are missing: the authority refresh on EVERY swap attempt; `before_erase` not called for `Creating`; a FOREIGN root alive at the creator's epoch is never cancelled (today 'cancel any foreign Creating row' passes the drain tests); each certificate kind through the drain; store error / undecodable slot at the probe; deposed leader makes no probe.
6. Smaller: `cas-decommission` can meet a row the leader just cancelled and throw CORRUPTED_DATA (`Tools/CasDecommission.cpp:371-378`); the comment about a lost cancel response does not match the behaviour (reviewers disagree whether the outcome is Cancelled or EntryChanged: settle by reading `casUpdateImpl`); 'nothing moves it' / 'terminal' are inexact (a later epoch of the same root may take the life over); `refresh_authority` mandatory with a no-op caller.
Unverified premise to keep in view: 'nothing is published under Creating' holds for ref appends by construction, but `checkPublicationAdmittedOrThrow` (`Pool/CasRefCatalog.cpp:780`) has no non-test caller and manifests are keyed by namespace, not life.

Soak evidence pending: the 12 h run started 2026-11-21 05:35 uses a binary with 90608d01636 (`tmp/intents/soak-merge/runs/combined-12h-20261121b`); its final drain shows whether rows of restarted creators are cancelled.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: cas-pr1 branch tip 90608d01636 (Cancel a stalled Creating catalog row whose creator is gone) implements the first version; isCreatorFenceTerminal is referenced from Pool/CasRefCatalog.h:311-312. Task notes: umbrella review 2026-11-21 verdict request changes, to ship in a separate PR. Changes: area set to gc; size unset -> medium (M): six review items on an existing commit; risk set to high; confidence set to solid; impact:cost; impact:operability; type unset -> bug; description: Update paragraph appended. Size: M. Would settle it: Whether items 1-6 of the review's required list are addressed in later commits; the commit is not pushed.
---
<!-- COMMENTS:END -->
