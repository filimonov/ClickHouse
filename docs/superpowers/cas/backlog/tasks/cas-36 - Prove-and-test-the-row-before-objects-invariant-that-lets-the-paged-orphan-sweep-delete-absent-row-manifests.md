---
id: CAS-36
title: >-
  Apply the mount-lease floor to every listed manifest in the paged orphan
  sweep, catalog row or not
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-10-12 11:52'
labels:
  - 'area:gc'
  - 'complexity:small'
  - 'risk:medium'
  - 'origin:2031-triage'
  - 'confidence:solid'
  - 'touches:gc-safety'
  - 'origin:noclock-audit'
milestone: m-2
dependencies: []
references:
  - CA/Gc/CasOrphanManifestSweep.cpp
  - 'https://github.com/Altinity/ClickHouse/pull/2528'
  - >-
    docs/superpowers/specs/2026-10-12-cas-orphan-sweep-floor-for-rowless-manifests-design.md
priority: high
type: bug
ordinal: 46000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`planManifestCursorPage` (`CA/Gc/CasOrphanManifestSweep.cpp`) skips `prefixEligibleUnder` for a listed key whose namespace has no row in the post-LIST catalog cut: the floor lookup sits inside `if (catalog_entry)`, and the key goes straight to `candidates`. The comment's premise ("creation publishes its row before any life-owned object") is false: `stageManifest` writes the body, and the `Creating` row appears only in `precommitAdd` via `namespaceLife` -> `createNamespace`. So a live build's freshly staged body in a namespace without a row (first write, or rebirth after row deletion) is deletable by the GC leader. `sweepNamespace` applies the floor first and unconditionally.
Today this is contained only by `promote` re-reading the body and failing closed on absence. PR #2528 (CAS-176.3) removes that read for the writer's own staged manifest, so the same race ends in a committed ref over a deleted body: the audit class returns to data loss (2031-triage CAS-022 downgraded it to P2 because of the re-read).
Decision (owner, 2026-10-12): the floor needs no catalog. The key carries the namespace, the namespace carries `server_root_id`, and `floorForNamespace` reads `gc/server-roots/<srid>/mount` from that alone. Run `prefixEligibleUnder` for every listed key and drop the `catalog_entry` guard around it. No lease at all means not eligible, as in `sweepNamespace`: decommission drains the victim's manifests under its claim and keeps the slot while debris remains, so a root with no lease has nothing left to reclaim. The farewell marker (`min_active_build_sequence == UINT64_MAX`) keeps dead-root debris eligible. The other `catalog_entry`-gated checks (recovery authority, `Creating`, owner view, §6 premise) stay as they are; they protect owned bodies, which a rowless namespace has none of.
Cost: one extra lease GET per rowless namespace per page, already cached in `floor_by_ns`.

Provenance: BACKLOG/gc.md#orphan-sweep-absent-catalog-row-window (2031-triage CAS-022), reframed 2026-09-26 as a proof task, reframed again 2026-10-12 as a fix after PR #2528. Verified 2026-10-12 against e98f1286 (PR #2528 head on antalya-26.6): floor block at `:761-775`, rowless nomination at `:885-920`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `prefixEligibleUnder` runs for every listed key in `planManifestCursorPage`; there is no `catalog_entry` condition around the floor lookup
- [ ] #2 gtest: a body in a namespace with no catalog row, under a live lease whose `min_active_build_sequence` is not above the key's build sequence, is retained (`deleted == 0`)
- [ ] #3 gtest: the same body under a lease with the farewell marker, or with a lower writer epoch, is deleted
- [ ] #4 `CursorPageDeletesObservedBodyWhenCatalogOmitsNamespace` is flipped: no lease at all retains the body, and the test comment states why (decommission drains under its claim)
- [ ] #5 The two sweep paths apply the same floor rule and the §6 comment no longer claims the row precedes the body
- [ ] #6 DECISION PENDING: whether `prefixEligibleUnder` treats `gc_fenced` like the farewell marker. The mount-lease series design (docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md, "Получение заново после фенса GC") decided the opposite: a fenced body `(E, m)` keeps protecting prefixes `(E, seq >= m)` until a new claim, to cover a ref-append of the old epoch that lands late. Resolve before implementing; the unconditional-floor fix does not depend on it
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
First recorded: 2026-08-21 (e811b3d68f4, by 'CAS-022')

2026-10-12: PR #2528 removes the `promote` body re-read for the writer's own staged manifest. That read was the only containment of this race (2031-triage CAS-022 §(d)). CAS-176.3 now depends on this task; land this before or together with #2528. Verified on e98f1286 that the floor needs only the namespace string: `floorForNamespace` derives `server_root_id` from it and reads the mount lease.

2026-10-12 (owner): `gc_fenced` must count as retired in `prefixEligibleUnder`, same as the farewell marker. Today a fenced lease keeps its last `writer_epoch` and `min_active_build_sequence` (fence-out copies the body and sets only the flag), and the predicate ignores the flag, so debris of a node that went offline waits for a remount with a new epoch or for a decommission; a node offline for a day keeps all of it. Safe because no writer of that epoch can have a request in flight: the writer admits a request only if it fits before its confirmed lease deadline minus `lease_safety_margin_ms` on `CLOCK_BOOTTIME` (`CasMountRuntime::admit`), and GC fences only after the token has held stable for `ttl + ttl/20 + cadence` (`mountObservationThresholdMs`). `claimMount` already treats `gc_fenced` and the clean marker identically for reclaim.

2026-10-12, correction: the `gc_fenced`-as-farewell item contradicts the accepted mount-lease series design r8 (0cb9107ed87), which keeps the fenced body protecting `(E, seq >= m)` on purpose (late-landing ref-append race). What a fenced lease actually holds is only debris with build sequence >= the last renewed `min_active_build_sequence`: builds in flight at death, plus everything started after a long-running build whose low seq pinned the floor. Builds with seq < m and all older epochs are eligible already. Owner to decide; AC #6 marked pending.

2026-10-12 noclock audit: reachability now verified in code by codex (docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-reflog.md finding 1; docs/superpowers/cas/2026-10-12-late-request-audit/objects/manifest.md S15). Path: `moveDirectory` → `republishRef` → `publishEntries` → `prepareEntries` → `stageManifest` runs before `namespaceLife(to_ns)`; GC samples no catalog row and nominates without a floor check (`planManifestCursorPage`); the writer then precommits and promotes; GC deletes the committed manifest body. Second fix option: create the namespace before staging (call `namespaceLife` before `stageManifest`, ~5–10 lines).

Same family as CAS-360 (writer and GC each act on a stale read); model work in CAS-375.
<!-- SECTION:NOTES:END -->
