---
id: CAS-357
title: >-
  Nested server-root ids let `floorForNamespace` judge one root's builds by
  another root's mount floor
status: To Do
assignee: []
created_date: '2026-10-12 09:50'
updated_date: '2026-10-12 09:58'
labels:
  - 'area:gc'
  - 'area:mounts'
  - 'complexity:small'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:review'
  - 'risk:high'
milestone: m-8
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2542'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h
priority: critical
type: bug
ordinal: 451000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`floorForNamespace` (`Gc/CasOrphanManifestSweep.cpp`) resolves a namespace's mount floor by trying every `/`-prefix of the namespace, longest first, and taking the first mount body that answers. Server-root ids may contain `/`; `validateServerRootId` (`Pool/CasServerRoot.h`) and `serverRootSubtreeEmpty` (`Pool/CasServerRoot.cpp`) do not forbid one id being a `/`-prefix of another: `a` first, then `a/b`, is accepted (the reverse is refused because `a`'s owned prefix `a/` matches `a/b`'s catalog rows). When the longer root's mount is not consulted, the lookup falls through to the shorter root's mount and judges the namespace's builds against an unrelated `writer_epoch` / `min_active_build_sequence` (epochs are per server root), and `prefixEligibleUnder` reports a live build retired.

Reachability before PR #2542: only while `a/b` has no `/mount` body at GET time. A member that has written a ref log has a mount (writes are gated on it), and the key is deleted only by the decommission tail after the catalog holds no victim namespaces, so a live writer could not reach it from GC.

Widened by https://github.com/Altinity/ClickHouse/pull/2542 (CAS-359): the lookup tries only roots in the caller's LIST (`HeartbeatFloor::listed_srids` from phase 3). A member `a/b` that mounts after that LIST is skipped although its mount exists.

The dangerous path is the fold's dead-precommit check (`Gc/CasGc.cpp`, `prefixEligible` under `foldManifestEdges` returning absent-body): round R lists `{a}` at phase 3 (`a`: `writer_epoch 2`, `min_active 500`); `a/b` claims at `writer_epoch 1` after the LIST, creates `a/b/store/465/<uuid>@cas@`, writes `AddPrecommit` for build `{1, 3}` (e.g. a relink fetch sharing blobs with `a`) while the body is still uploading; phase 4's catalog read includes the namespace; the fold finds the body absent, `prefixEligible` skips `a/b/store/465`, `a/b/store`, `a/b` (unlisted), reads `a`'s mount, `1 < 2` → eligible → `CASGCDeadPrecommitSkipped`, the cursor advances past the log. `Promote` emits no edge, so the part's only `+1` is gone for good; when `a` later merges the shared part away, its `-1` condemns blobs `a/b` still references. Data loss.

Backstops that hold: the orphan sweep page (a namespace without a coverage row is retained as `NoCoverage`; a covered namespace implies an earlier, listed mount), `rebuildBaseline` (lists roots after `seen_ns` is formed), fsck (report label only), and every non-nested layout (an unlisted root gives `nullopt`, the fail-closed direction).

Pre-existing nesting hazards outside GC: `ownsNamespace` and decommission's `victim_namespace_prefix = srid + "/"` treat `a/b/...` as owned by `a`.

Fix options: (a) reject nested ids at the owner claim, both directions, in the emptiness check, and document it next to `cas_server_root_id`; closes the GC, decommission and `ownsNamespace` hazards. (b) never fall through past an unlisted longer prefix: restores the old GETs for every namespace, defeats the PR. (c) keep the listed-only lookup for the sweep page (backstopped by `NoCoverage`) and leave the fold's dead-precommit check on the probe-every-prefix lookup; it fires only on absent-body precommits, so its cost is negligible. Recommended: (c) in PR #2542 now, (a) as the durable fix.

Found in the review of PR #2542 on 2026-10-12 (head be3da0cd71f, base antalya-26.6 @ 0f732b6b697); independent ca-review confirmed the fold path and traced the blob-loss consequence.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A member whose id is a `/`-prefix or `/`-extension of an existing member is refused at mount, or the floor lookup never falls through past an unlisted longer prefix (decision recorded in the task)
- [ ] #2 A gtest with members `a` and `a/b` where `a/b` is absent from the caller's listing: the sweep retains `a/b`'s builds, the fold keeps the precommit edge, fsck does not report reclaimable-pre-precommit
- [ ] #3 The rule is documented in docs/en/antalya/cas/configuration.md next to `cas_server_root_id`
- [ ] #4 Failing-first gtest: fold over a namespace of srid `a/b` unlisted while `a` is listed, precommit with absent body: the barrier holds (no `CASGCDeadPrecommitSkipped`)
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
