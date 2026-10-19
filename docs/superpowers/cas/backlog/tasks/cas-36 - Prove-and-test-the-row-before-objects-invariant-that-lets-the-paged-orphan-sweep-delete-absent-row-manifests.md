---
id: CAS-36
title: >-
  Apply the mount-lease floor to every listed manifest in the paged orphan
  sweep, catalog row or not
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-10-19 20:46'
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
- [ ] #1 `planManifestCursorPage` samples the floor for every listed namespace in a prepass BEFORE `CasRefCatalog::read` (not only before `catalogEntryOf`), keeps those samples for the page, and `prefixEligibleUnder` runs for every listed key with no `catalog_entry` condition; the §4 snippet of docs/superpowers/specs/2026-10-12-cas-orphan-sweep-floor-for-rowless-manifests-design.md is superseded by CAS-358 spec §4.4. gtest: a hook right after the catalog read returns a rowless cut lets the writer create the row, precommit, promote, end the build and publish a higher floor; the body is retained, and the reversed-order mutant (catalog read before the floor) deletes it and fails the test
- [ ] #2 gtest: a body in a namespace with no catalog row, under a live lease whose `min_active_build_sequence` is not above the key's build sequence, is retained (`deleted == 0`)
- [ ] #3 gtest: the same body under a lease with the farewell marker, or with a lower writer epoch, is deleted
- [ ] #4 `CursorPageDeletesObservedBodyWhenCatalogOmitsNamespace` is flipped: no lease at all retains the body, and the test comment states why (decommission drains under its claim)
- [ ] #5 The two sweep paths apply the same floor rule and the §6 comment no longer claims the row precedes the body
- [ ] #6 `prefixEligibleUnder` treats `gc_fenced` like the farewell marker: every build sequence of a fenced lease's epoch is eligible (gtest: rowless body under a `gc_fenced` lease with `min_active_build_sequence` at or below the key's sequence is deleted; gtest: a `+1` precommit edge with an absent body under a `gc_fenced` lease is skipped as `dead_precommit_skipped`, not clamped)
- [ ] #7 The sentence in docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md ("Получение заново после фенса GC") that says the fenced body keeps protecting `(E, seq >= m)` is amended to the new rule, with the argument from the CAS-36 spec §6
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

2026-10-12 (owner, final): `gc_fenced` counts as retired. The series r8 sentence guarded a late-landing `precommitAdd` append of the fenced epoch; the fold already resolves that case with the same predicate: a `+1` precommit edge whose body is absent and whose build is eligible is skipped as non-activating (`CASGCDeadPrecommitSkipped`, `Gc/CasGc.cpp` fold_ref_intake), and its binding is removed by the successor's stale-precommit sweep (`-1` of a never-activated precommit emits nothing). A `promote` append in flight is covered by the owner view (live precommits are protected) regardless of the floor. So sweep and fold agree under the new rule, no barrier and no data loss; what changes is that the dead node's in-flight debris is reclaimed before a remount or decommission. AC #6 is firm, AC #7 places the series amendment.

2026-10-12 model result (`a385db56107`, `docs/superpowers/models/CaGcRootLocalPartManifestCore_RESULTS.md` section CAS-36): the fused `GOrphanSweep` is split into `GSwList`/`GSwCatalog`/`GSwFloor`/`GSwView`/`GSwNominate`/`GSwDelete` under `EnableCatalogRows`. Current protocol violates `INV_NO_DANGLE` in 11 states (`cur_rowless`): stage body with no row → GC LIST → catalog cut (no row) → writer creates the namespace, precommits, promotes → sweep skips the floor for a rowless namespace → nominates → deletes the committed body.

Reachable in code at the base on two paths: table move (`moveDirectory` → `republishRef` → … → `stageManifest` before `precommitAdd` births the row; `namespaceLife(to_ns)` runs after the loop and only with namespace files) and the first INSERT into a CAS disk that is not the policy's first writable disk (`format_version.txt` went elsewhere, so no row yet). `namespaceLife` rebirth also applies.

Candidates: (A') this task's wording — floor on every listed key — is NOT enough: the floor is read after the catalog cut, so a build can be born, precommit, promote and end in between (`fix_floorall` violates, 12 states). (A'') floor read for every distinct namespace on the page BEFORE `CasRefCatalog::read`, no lease = not eligible: green (`fix_floorfirst`), load-bearing (`sab_floorfirst`). (A) `namespaceLife` before `stageManifest` works only with a life pin in `precommitAdd`/`promote` (row removal + rebirth reopens it; `sab_stageafterbirth_nopin`). (C) `If-Match` delete has no effect: the key is write-once, the recorded token is the committed body's token.

Recommended: A'' (GC only, in `planManifestCursorPage`): one mount-lease GET per distinct namespace per page, no writer change. A + pin optional defense in depth.

Open: AC #6 counts `gc_fenced` as eligible, which assumes no late `precommitAdd`/`promote` of a fenced epoch can land (lease-admission assumption). A nomination also retires the manifest's blob source edges in the same CAS (`orphan_source_retirements`), so the same race can unprotect its blobs (not modelled). Correction: at the base the orphan delete (PHASE 18) is HEAD + exact-etag `remove`; `removeManyWriteOnce` is the owner-removed path (PHASE 15).

2026-10-12 decision after the two-model consult (`tmp` notes summarized here): fix = A'' — in `planManifestCursorPage` read `floorForNamespace` for every distinct namespace on the page BEFORE `CasRefCatalog::read` and before any owner-view read; apply `prefixEligibleUnder` to every key, catalog row or not; no lease = not eligible; never fetch a missing floor after the cut (budget-limited unsampled namespaces are retained); rewrite the false row-before-objects comment. GC-only, protects old writers once the leader is upgraded. Rejected: A' (floor after the cut), C (`If-Match` delete; key is write-once), A + life pin (larger, writer upgrade, no proven value).

AC #6 (`gc_fenced` counts as retired) stays as decided by the owner. Opus objected that a rowless namespace has no owner view, so a late `namespaceLife` + `precommitAdd` + `promote` of a fenced epoch can land between nomination and delete (the store does not fence it: the namespace is new, no seals). Excluding `gc_fenced` does not remove that dependency: after an ordinary remount the epoch is superseded (`prefix.writer_epoch < w.writer_epoch`) and the same late request lands the same way; excluding it only defers reclamation until an epoch change that a dead node may never get. Residual, documented: for rowless namespaces a late request of a fenced or superseded epoch is excluded only by lease admission — the same class as the holder-death certificate and CAS-343. Model work: add `GFence(e)` and a late request of epoch e landing after `GSwFloor`; expect a violation only when the request lands after its admission deadline. Also add blob source edges and `orphan_source_retirements` at nomination with a committed-manifest blob-reachability invariant.

CAS-358 reshapes the same function and depends on this task: its owner view (fold set `as_of` the cursor + committed tail above it) must be read AFTER the floor sample, and rowless namespaces must be decided by the floor alone.

2026-10-12 model follow-up (`eae836fd008`, RESULTS 'Residual' headings): with explicit admitted-but-unresolved requests (`WAdmitOp`/`WResolveOp`), `GFence`, `WRemount`: `fenced_late_bound` and `superseded_late_bound` green, `fenced_late_unbounded` (13 states) and `superseded_late_unbounded` (14) violate `INV_NO_DANGLE`. Correction to the residual wording above: it is NOT limited to rowless namespaces — the shortest fenced trace has a catalog row; the owner view (`activeManifestKeys`) is one read and a late `precommitAdd`/`promote` of the fenced/superseded epoch lands after it and before nomination. A'' is safe only under the admission bound, for any namespace. Excluding `gc_fenced` does not help (remount fails the same way; `EpochSeal` is lazy and untouched namespaces never get one). New invariant `INV_COMMITTED_BLOBS_PRESENT`: the current protocol also loses a committed blob through `orphan_source_retirements` (`cur_rowless_blobloss`, 19 states); A'' keeps it green. Witness `witness_fencedswept`: the fenced arm still reclaims debris.

2026-10-12, PR #2528 head f79cc0df (commits a923533e, f79cc0df): `planManifestCursorPage` now runs the floor for every listed key (the `catalog_entry` guard is gone) and, for candidates whose namespace had no row in the post-LIST cut, re-reads `CasRefCatalog` once per page after the floor reads and retains any candidate whose row appeared. This is the A'' guarantee in a different order: eligible at the floor read F implies the build ended before F, so its row birth precedes the recheck C' > F; rowless at C' and eligible at F is dead debris. Cost: one extra catalog GET per page only when a rowless candidate survives the floor. Tests: `CursorPageRetainsRowlessBodyWithoutMountLease` (flip of the old test, AC #4), `CursorPageRetainsRowlessBodyOfActiveBuild` (AC #2), `CursorPageDeletesRowlessBodyBelowFloor` (AC #3, min_active only), `CursorPageRetainsRowlessBodyWhenNamespaceAppearsAfterCatalogCut` (the fix_floorall schedule). Not in the PR: farewell/lower-epoch cases of AC #3, the why-comment of AC #4, AC #6 (gc_fenced), AC #7, the end-to-end Pool test, user-doc flow chart. The CAS-36 spec rev.2 §4 still describes the floor-after-cut shape without the recheck; it needs rev.3 (A'' or the PR's C/F/C' shape).

2026-10-14: 'a root with no lease' now means only a never-mounted root. A decommissioned srid keeps a terminal farewell.

2026-10-14, PR #2528 head 7d7fcc95 (e198e5ee, 593311dc, 68cdc1d9, merge of antalya-26.6, 7d7fcc95): `prefixEligibleUnder`/`prefixEligible` renamed to `isBuildPrefixCoveredByFloor`, `floorForNamespace` to `mountLeaseForNamespace` (AC #1/#6 wording refers to the old names). The post-floor catalog recheck now covers every candidate, not only rowless ones: a candidate is retained when its catalog row appeared, disappeared or changed by full value (`CatalogEntry::operator==`) between the cut and the recheck. Closes the rebirth schedule: cut sees the predecessor life, its owner view is empty, the successor life commits the same manifest before the floor read, the old code nominated the body (gtest `CursorPageRetainsBodyWhenNamespaceIsRebornAfterCatalogCut`). Cost: one catalog GET per page that has any candidate. The explanatory comment on the recheck was dropped with the generalisation; worth restoring. Separate finding by the author: the decoder rejects entry paths with empty/'.'/'..' segments (`CasPartManifestFormat.cpp` decodePartManifest) and the encoder does not, so the dropped promote re-read also drops that check; red test `StageManifestRejectsTraversalPathBeforeBodyWrite` is in, the fix is not yet pushed (CI red on it as intended). CI for 7d7fcc95: that unit test plus an unrelated stateless crash (03278, `GroupingAggregatedTransform` bucket pushed twice, upstream issue ClickHouse/ClickHouse#115986).

- 2026-10-14: CAS-358 spec rev.3 (codex round 7, finding 2): moving the floor only ahead of catalogEntryOf keeps CasRefCatalog::read at CasOrphanManifestSweep.cpp:704 before the floor, so a build that ends between the catalog read and the floor sample is still deletable through the rowless branch. Amend this spec's §4 to a page prepass: floors for every listed namespace BEFORE the catalog read; T1 hook must fire right after the catalog read returns the rowless cut, and the reversed-order mutant must fail.

2026-10-19, PR #2528 head 08a90873 (657c521c + merge of antalya-26.6): the dropped path-hygiene check is restored at the write boundary: `validateManifestEntryPath` in `Formats/CasPartManifestFormat.cpp` is now shared by `encodePartManifest` and `decodePartManifest`, so `stageManifest` rejects `../evil`, `/abs`, empty, `a//b`, `a/./b` before the PUT (gtests `EncodeRejectsMalformedEntryPaths`, `StageManifestRejectsTraversalPathBeforeBodyWrite`; the decode test now forges bytes). The author's own review (gist 7b34b402f18b051ced88f457c95d5df4, at 593311dc) also asked for: the 64 KiB entry-record cap at encode time (added in 470fb0e4, removed again in 17dc11cc; decoder-only cap remains, so an over-cap record is still committable without a re-read), a `retained_catalog_changed` counter for the recheck retention (not done; it lands in the generic `skipped`), and the PR body/changelog naming the orphan-sweep safety fix (not done; body still 'Performance Improvement', still says 'proof'). CI 08a90873: unit, stateless cas s3 and integration green; AST fuzzer old_compatibility = docker pull timeout; stress (asan_ubsan, cas s3) 'Cannot start clickhouse-server' = `Hybrid` engine from the antalya-26.6 merge stores `remote(..., currentDatabase(), ...)` unevaluated and re-resolves it to `default` on ATTACH after restart (03643_hybrid.sql ran under test_5), not CAS.
<!-- SECTION:NOTES:END -->
