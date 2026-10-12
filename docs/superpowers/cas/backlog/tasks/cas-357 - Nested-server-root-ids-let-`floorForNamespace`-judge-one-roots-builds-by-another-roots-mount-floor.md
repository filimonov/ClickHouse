---
id: CAS-357
title: >-
  Nested server-root ids let `floorForNamespace` judge one root's builds by
  another root's mount floor
status: To Do
assignee: []
created_date: '2026-10-12 09:50'
updated_date: '2026-10-12 09:50'
labels:
  - 'area:gc'
  - 'area:mounts'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:review'
milestone: m-8
dependencies: []
references:
  - 'https://github.com/Altinity/ClickHouse/pull/2542'
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h
priority: high
type: bug
ordinal: 451000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`floorForNamespace` (`Gc/CasOrphanManifestSweep.cpp`) resolves the mount floor of a namespace by trying every `/`-prefix of the namespace, longest first, and taking the first mount body that answers. Server-root ids may contain `/` and `validateServerRootId` (`Pool/CasServerRoot.h`) does not forbid one id being a `/`-prefix of another (`a` and `a/b` can both be members). When the longer root's mount body is not found, the lookup falls through to the shorter root's mount and judges the namespace's builds against an unrelated `writer_epoch` / `min_active_build_sequence` (epochs are per server root). `prefixEligibleUnder` then reports an in-flight build retired when `W_a > E_ab`, and the orphan sweep can nominate its manifest body. The same misattribution reaches the fold's dead-precommit skip, `rebuildBaseline` and fsck's `reclaimable-pre-precommit` class.

Reachability before PR #2542: only while `a/b` has no `/mount` body at GET time (never mounted, or decommissioned with debris left under `a/b/...`).

Widened by https://github.com/Altinity/ClickHouse/pull/2542: the lookup tries only roots the caller LISTed (`HeartbeatFloor::listed_srids` from phase 3, `listServerRoots` elsewhere). A member `a/b` mounted after phase 3's LIST whose namespace is swept on a phase 9 page in the same round is skipped even though its mount exists, and the page uses `a`'s floor. Concrete: `a` listed with `writer_epoch = 7`; `a/b` re-added after `DROP POOL MEMBER`, mounts at epoch 1 after the LIST, starts build `(1, 3)` under an old namespace with a coverage row; the page finds `a/b` unlisted, reads `a`'s mount, `1 < 7` → eligible; the manifest is pre-precommit, so it is not in the ref table → nominated.

Options: (1) forbid nesting at mount time (`claimOwnerOrThrow` LISTs `gc/server-roots/` once and refuses an id that is a `/`-prefix or `/`-extension of an existing member; document in `docs/en/antalya/cas/configuration.md`); (2) make the lookup stop at the first unlisted prefix instead of falling through. (1) also closes the pre-existing window. Decide before or together with PR #2542.

Found in the review of PR #2542 on 2026-10-12 against the PR head be3da0cd71f (base antalya-26.6 @ 0f732b6b697).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A member whose id is a `/`-prefix or `/`-extension of an existing member is refused at mount, or the floor lookup never falls through past an unlisted longer prefix (decision recorded in the task)
- [ ] #2 A gtest with members `a` and `a/b` where `a/b` is absent from the caller's listing: the sweep retains `a/b`'s builds, the fold keeps the precommit edge, fsck does not report reclaimable-pre-precommit
- [ ] #3 The rule is documented in docs/en/antalya/cas/configuration.md next to `cas_server_root_id`
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
