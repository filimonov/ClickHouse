---
description: 'Live backlog — architecture/refactoring (no behavior change), documentation debt, and minor/polish items.'
sidebar_label: 'Docs & cleanup'
sidebar_position: 9
slug: /superpowers/cas/backlog/docs-and-cleanup
title: 'CAS Backlog — Docs and cleanup'
doc_type: 'guide'
---

# CAS Backlog — Docs and cleanup {#docs-and-cleanup}

Part of the [CAS live backlog](/superpowers/cas/backlog). Topic file for deferred architecture/
refactoring work (no behavior change), documentation debt, and minor/polish items.

## Architecture / refactoring (deferred, no behavior change) {#refactoring}

- **[refactor: CasGc split]** — KEEP — Split scan/reachability/deletion/cursor/budget out of `Gc/CasGc.cpp` (4861 lines today, grew since last review); keep `Gc` as pure orchestration. Tracked at [PR #2286](https://github.com/Altinity/ClickHouse/pull/2286) (open).
- **[refactor: Store de-god-classing]** — KEEP — `Cas::Store` was renamed to `Cas::Pool` (old name is dead). `Pool/CasPool.cpp`+`.h` is now 3461 lines; caches and the ref-log lane already split out into `CasManifestReader`/`CasRefLedger`, but the remount thread is still inline. Two small extraction candidates remain unclaimed: `listNamespaces`/`listMirroredChildren` (~112 lines, `CasPool.h:618,624`) and the anomaly-policy pair `reportImpossibleInterference`/`peekForeignRefLogHeader` (~113 lines, `CasPool.h:933`), deliberately kept inline by the mount plan. Extracting both would land near 3236 lines, still dominated by the mount protocol; low priority, the composition root is sound as-is (formerly `[source-layout-casstore-followups]`).
- **[DiskSelector per-disk isolation]** — KEEP, upstream — `DiskSelector::initialize` (`src/Disks/DiskSelector.cpp:92-141`) has no per-disk try/catch inside its `for` loop; one unreachable disk still aborts disk-selector init server-wide (confirmed unchanged on `cas-gc-rebuild` and `altinity/antalya-26.6`). Pre-existing upstream gap; carve to Group G.
- **[Group G] carve generic Ring-2 fixes into separate upstream PRs** — {#refactor-group-g} — KEEP (fork hygiene) — Shrinks the fork's long-term conflict surface: `ThreadStatus parent_thread_group` (B90), `ReadBufferFromFileView` (B115), `ReadBufferFromS3` cancel-stop (B117), `LocalObjectStorage` TOCTOU (B38), `MergeTreeDeduplicationLog` null-writer (B37), `copyS3File message_format_string`, `Expect:100-continue` opt-in, `S3Exception::isPreconditionFailed`, GCS conditional dialect + GOOG4 signer, generic conditional-S3-write plumbing, and the `clickhouse-disks --query` non-interactive exit-code contract change (`f85cb4330c8`, still rides in the CAS branch — full record at `operability-and-introspection.md#disks-exit-code-upstream`). Non-blocking.

## Refactoring candidates, derived from what actually broke {#refactor-candidates-from-defects}

Ranked by value-per-risk, each backed by real defects it would have prevented. Item 1 ("make catalog-life
absence expressible via `std::optional`") and item 5 (a single named fixture seam for nonproduction test
shapes, `b5c812ba56a`, `src/Disks/tests/cas_test_helpers.h:1109` `namespace fixture`) are DONE and removed.

1. **One life resolution per round, threaded — not re-derived.** `Gc::fold`'s `live_incarnation` map (`Gc/CasGc.cpp:1703-3065`) is real and threaded through the round, but `Tools/CasFsck.cpp`/`Tools/CasDecommission.cpp` still call `CasRefCatalog::read` independently rather than consulting it.
2. **The destructive gate collapses per-namespace facts into a pool-wide boolean.** `FoldResult::suppress_destructive` (`Gc/CasGc.h:616`) is still one scalar OR over every namespace's anomalies/holds/frontier state; its own comment already flags "Stage B's narrowing of this gate to per-namespace" as the open follow-up.
3. **`Gc/CasGc.cpp` (4861) + `Pool/CasRefLedger.cpp` (5353) = 17.2% of the subsystem** by line count (was ~18%, both files have since grown in absolute size) — extraction needs equivalence fences written before the move.
4. **Keep converting prose rules into executing checks.** Two conversions already hold (`FsckReport`'s `static_assert(kFsckHardFindings.size()==5,...)`, `Tools/CasFsck.h:244`; the `CAS*` gtest gate policy, AGENTS.md §3) — every remaining "whenever X, also do Y" code comment is a candidate.

## Minor / polish {#minor}

- **[RUSTFS-ERROR-XML]** — KEEP — Every RustFS `PreconditionFailed` still logs `Unable to parse ExceptionName: ...`; `S3::isPreconditionFailedError` (`src/IO/S3/Client.cpp:113`) still recognizes it from message TEXT, not a shape-tolerant XML parse. Add one before relying on it wider.
- **[F4]** — KEEP — CA `MOVE PARTITION` still publishes ref CAS before validating the target disk is in the storage policy (`MergeTreeData.cpp:6995-7109`); validate in-policy first.
- **[Ring-2 comment/convention nits]** — KEEP — 6/9 sub-nits spot-checked still open; 3 not independently re-verified this pass: `S3Common.h` comment overclaims for the RetryStrategy site; `ProfileEvents.cpp` changelog fragment in a description; `_ms` suffix on a `DateTime64(3)` column (confirmed still open, `StorageSystemContentAddressedMounts.cpp:192-193`). Confirmed still open: no `static_assert(DEFAULT_EXPECT_CONTINUE_MIN_BYTES==0)` (`src/IO/S3Defines.h:43`); `MergeTask::projection_uses_parent_transaction` still a member (`MergeTask.h:225`); internal `cas_part_folder_cache_*` names outlived the key rename (`ContentAddressedMetadataStorage.cpp:307-309`); `SYSTEM CAS GC REBUILD` still abbreviates "GC" vs the spelled-out sibling; `05011_cas_gc_rebuild_access.sh` still tagged `no-parallel`; untracked empty `poc/` dir husk still present.
- **[C2-followups]** — KEEP, narrowed — `ListedKeyFn` (`Backend/CasRequests.h:133`) already returns `bool`, so the stop-on-true page-boundary hook the item asked for now exists. `CasServerRoot.cpp`'s listing loops already use `forEachListedKey`; `CasRefIntake.cpp` no longer exists (folded into `CasRefLedger.cpp`). Remaining: `deletePrefixWholesale` (`Gc/CasGc.cpp:3725`) still hand-rolls its own `op.list` pagination loop instead of using the hook.
- **[stale-recover-ref-table-comments]** — KEEP — Comments still name the dead `recoverRefTable`/`recoverRefTableDetailed` at `Gc/CasOrphanManifestSweep.cpp:35,172` and `Gc/CasGc.cpp:84` (drifted from `:32/:167/:80`); the real function is `recoverRefTableDetailedFromAuthority`. Comment-only fix.
- **[fsck-short-keys-spell-out]** — KEEP, USER DECISION pending — short keys `ns`/`me`/`p`/`ha` in `CasEvent::detail`/fsck-report maps still undecided (spell-out vs keep).
- **[prev-indeg-rename-never-happened]** — record, no action — commit `60691b11e7f`'s message claims a `prev_indeg`→`prev_indegree` rename that never happened (the key had no emitter); commit messages are immutable, this entry is the durable pointer.
- **[casrequestcontrol-comment-settings-stale] OBSOLETE: `CasRequestControl` deleted wholesale (`7f2a3b03a460`, `cas-gc-rebuild` only; the file still exists on `antalya-26.6`)** — DOC — Found during the Task 12 fix round: the header comments name `cas_s3_retry_initial_backoff_ms`/`cas_s3_retry_max_backoff_ms` as if they were configurable settings; they exist only in the comment text — the real budget is hardcoded in `CasRequestBudget`. Either implement the settings or fix the comments to stop implying a configuration surface that isn't there. `CasRequestControl.{h,cpp}` and its dedicated test are deleted on `cas-gc-rebuild`, so the stale comments no longer exist there; the item stays open on `antalya-26.6`, where the file is unchanged.
- **[s3cache-config-comment-stale] stale comment in `utils/ca-soak/configs/storage_conf_s3cache_ch1.xml`** — MINOR — The comment claims cache-over-CA fails with `NOT_IMPLEMENTED`; this was fixed by `3ed0e5f5030` (2026-07-08) and the cache-over-CA path is now live-validated (see the quick-start cache example, `380688e8a66`). Remove the stale comment.
- **[part-folder-validate-never-gating] ✅ CLOSED by the retirement of `part_folder_validate` (`66b480241b7`, 2026-09-03)** — HARD (user settings-policy direction) — The setting this item demanded a gate for no longer exists: the manifest-cache-by-id work retired `part_folder_validate` entirely, so there is no `never` value left to silently accept. `RetiredPartFolderValidateIsRejected` pins that loading the retired name now throws `UNKNOWN_SETTING`.
- **Spec drift (`ensureBlobPresent`).** `docs/superpowers/specs/2026-09-02-cas-backend-token-contract-design.md` (revision 13)
  still prescribes `op.publish(…, Retry::once())` and "never the shared `standard`" for
  `ensureBlobPresent`; since the loop-deadline fix the publish runs under the loop's frozen policy made
  single-attempt (one physical attempt, bounded by the loop's one deadline). Reword the spec sentence. Owner: the spec's next revision (revision 14), one editing pass for all three recorded drifts (this one, `[spec-drift-ref-lane-once]` below, and the `isAccessTokenExpiredError` sentence in the next bullet).
- **Spec drift (`isAccessTokenExpiredError`).** The spec's retry section names `S3Exception::isAccessTokenExpiredError` as the
  refreshability predicate; the landed `isRefreshableCredentialError` is deliberately narrower (named codes
  only, never `S3Errors::UNKNOWN`), because the general predicate would turn every unmodelled store answer
  into a refusal. The spec sentence is stale; the ruling is recorded in
  `docs/superpowers/cas/2026-09-03-request-contract-rulings.md`. Fix: reword the spec sentence to name the
  CAS-local predicate and its reason. (Codex production review, 2026-09-03, adjudicated; the review's 3
  confirmed defects were already fixed in the plan's fix round, and its "accepted request costs" bullet is
  recorded in full in that same rulings doc.)
- **U9 `reconcileMetaClean` comment (CP3/Task 7 review, 2026-09-03).** `Pool/CasPartWriteTxn.cpp`'s
  `reconcileMetaClean` create-first gate comment over-states what an absent observation implies (an absent
  blob-body observation does not imply an absent marker).
- **U6 `CasRefCatalog.cpp` citation (CP3/Task 7 review, 2026-09-03).** `Pool/CasRefCatalog.cpp:655` still
  cites "the Task 2 review's own note on `casAdmitEntry`" — the header and test-file sweep were cleaned,
  this `.cpp` site was missed.
- **Prose and spec drift from the engine fix round review, 2026-09-03 (4 sites, `Backend/CasRequests.{h,cpp}`):**
  - `isDefinitelyRefusedWrite`'s doc still says the engine refuses when "there is no reissue left to sign with what it did install" — under `once` no refresh is invoked any more, so that disjunct is unreachable; drop it. (FALSE)
  - `WriteState::any_ambiguous` comment: "an inner write that ended in `Conflict` saw the precondition move" is false of the `!any_ambiguous` arm, which returns `Conflict{NotObserved}` having proved nothing — scope the claim to the ambiguous arm.
  - `writeLoop` reset comment: "sent DIFFERENT bytes" is not guaranteed (`decide` may repeat bytes); the proof is the observed precondition, not the byte difference.
  - `admit`/`resume` thread-safety comment: the conclusion is right, the enumeration is not (neither reads the backend; `resume` reads no member).

### Spec drift: the ref-lane inventory row says `standard`, the coverage-gate paragraph and the code say `once` (2026-09-04) {#spec-drift-ref-lane-once}

`docs/superpowers/specs/2026-09-02-cas-backend-token-contract-design.md` (revision 13) lists "the ref lane
(`commitRefChunk`, the recovery walk, `resolveWedgeOnce`) — `create` under `standard`" in the inventory
table, while its coverage-gate paragraph states that "the `once` writes of the pulse and the wedge retry
are never a key's first request" and that the recovery walk's epoch seal at `T+1` is a `once` write.
The implementation follows the paragraph (`resolveWedgeOnce` and the recovery seal `create` under
`Retry::once`; the lane's own next flush is the retry), as chosen in the migration's ref-ledger unit and
approved by its review. Fix: reword the inventory row to say `commitRefChunk` under `standard`, the wedge
retry and the epoch seal under `once` with the reason. Found by the external test review (tests-02 #7/#8).

## `[manifest-cache-by-id-prose-batch]` manifest-cache-by-id: prose and naming batch, still unapplied {#manifest-cache-by-id-prose-batch}

From the whole-branch review of the manifest-cache-by-id work
(`docs/superpowers/specs/2026-09-02-cas-manifest-cache-by-id-design.md`). All prose or a single
identifier rename, none blocking; re-verified 2026-09-25, none of the 9 applied yet. **Not compacted to
a paragraph — each line below is already the minimal actionable unit (a literal find-and-replace);
folding these into prose would require re-expanding them for whoever applies the fix.**

1. `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.cpp:450`,
   `CachedPartFolderAccess::prepareEntries`. Current: "No pool HEAD/GET is performed before precommit;
   the promote path re-proves each dependency fail-closed." False: `promote` re-checks no `Materialized`
   leaf and probes no `TrustedManifest` leaf. Replace with: "the promote gate requires a dependency
   proof for every blob leaf and a live precommit owner; it probes no blob, a missing adopted body is
   fsck's to report."
2. `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:1210`,
   `createHardLink`'s carry-forward comment. Current: "record a TOKENLESS W-EVIDENCE dep for its blob
   (no HEAD before precommit; promote re-proves it)." Same false claim as the `prepareEntries` one
   above. Replace with: "record a TOKENLESS W-EVIDENCE dep for its blob (no HEAD before precommit; the
   promote gate requires a dependency proof for every blob leaf and a live precommit owner — it probes
   no blob, a missing body is fsck's to report)."
3. `src/Disks/tests/gtest_cas_pool.cpp:131`, the `publishPartWithEntries` helper comment. Current: "Each
   Blob entry's body MUST be present at promote: the promote gate revalidates EVERY blob leaf with a
   HEAD and fails closed on an absent body." False: promote's `TrustedManifest` arm issues no probe.
   Replace with: "Each Blob entry's body MUST be present at promote: the promote gate requires a
   dependency proof for every blob leaf and fails closed if one is missing — it does not itself HEAD or
   GET the body."
4. `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h:66`, the
   `Freshness::StrictValidate` enumerator comment. Current: "fsck/debug: bypass retained views entirely;
   fresh resolve + validated read." Overclaims: `StrictValidate` now does nothing beyond a `ForceFresh`
   resolve except skip the retained-view cache. Replace with: "fsck/debug: fresh resolve that bypasses
   the retained view cache entirely, populating nothing; otherwise identical to `ForceFresh`."
5. `src/Disks/tests/gtest_cas_part_folder_access.cpp:230`,
   `HitPathJournalEmptyAndCheapWhenExplainDisabled`. Current: "Same request oracle as
   `RetainedHitCostsNoRequest` — one cold build, then retained hits." Stale since `5973676fbad`, which
   moved `RetainedHitCostsNoRequest` to five zero totals excluding the cold build; this test still
   asserts `getCount == 1` over the cold build plus hits. Replace with: "One body GET across the cold
   build and five hits; the full no-request oracle is `RetainedHitCostsNoRequest`."
6. `docs/en/antalya/cas/operations/troubleshooting.md:28`, the "Stale-looking part metadata" row's cause
   cell. Current: "The part-folder view cache may be serving a retained (not re-validated) view." A
   retained view is validated by manifest id against a fresh resolve on every hit; the snapshot that can
   now outlive an out-of-band change is the manifest decode cache, which the row's fix cell already
   names. Replace with: "The part-folder view cache or the manifest decode cache may be serving a
   snapshot taken before the out-of-band change."
7. `src/Common/ProfileEvents.cpp:929`, `CASPartFolderManifestGets`'s description. Current: "Number of
   part-manifest body GET requests used to build or validate folder views. High values indicate cache
   misses or validation work." No GET validates anything now; the counter increments once per manifest
   decode-cache miss. Replace with: "Number of part-manifest body GET requests, one per manifest
   decode-cache miss."
8. STALE (2026-09-26): this point originally described `BACKLOG/performance.md`'s
   `{#hardlink-per-file-forcefresh-head}` (lines ~306-322) as still asserting, present tense under the
   `✅ CLOSED` banner, that `ForceFresh` "never serves a retained view" and the reader's `HEAD` "is
   mandatory even on a decode-cache hit." That file was independently groomed and rewritten
   (`0bf85b2d3ee`, `f8aef5bef2f`): the section now reads "— DONE (provenance kept)" with a short,
   already-trimmed body and neither false claim present. Nothing left to apply for this point.
9. `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:1619-1627`
   (`unlinkFile`'s `already_proven` memo) and
   `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h:166`
   (`force_fresh_validated_refs`). The surrounding comments were rewritten from "re-proven" to
   "resolved" while the identifiers still say proven/validated — the memo now saves a fresh RESOLVE, not
   a proof. Rename `force_fresh_validated_refs` to `force_fresh_resolved_refs` and `already_proven` to
   `already_resolved` (or fold into the memo's removal, if that happens first).

## Source-layout refactoring residue (2026-07-16) {#source-layout-residue}

- **[source-layout-bisect-hazard]** — KEEP, record — intermediate commits `592b9b8..9d714dd8` are not clean-buildable (a Phase-2 include sweep stranded 3 external-consumer fixes outside the sweep's pathspec); accepted as-is (no-amend/no-rebase rule). Lesson: a move/sweep's pathspec must include every touched file including external consumers, and the committed state, not an incrementally-built tree, must be what's verified green.
- **[phase4-blob-uploader-descoped] SUPERSEDED DECISION RECORD — the old recursive blob lane was replaced, not extracted** — The original Phase 4 was correctly descoped for the code that existed then: conditional-create failure doubled as discovery, entangling byte delivery with adoption/displacement. The 2026-08-23 unconditional-publication rewrite later removed that machine entirely. `PartWriteTxn::ensureBlobPresent` now owns the explicit `HEAD`/metadata/publication decision, while backend `publishBlob` owns transport-only streaming or native copy. This is the separation the old extraction wanted, achieved by a reviewed protocol simplification rather than by moving the former code. No further `CasBlobUploader` extraction is scheduled; the identifier and old decision remain recoverable in git history.
- **[source-layout-build-naming]** — KEEP, low-pri — After `Build`→`PartWriteTxn`, several helper names still say "Build" as an English/protocol word (`promoteBuild`, `registerInflightBuild`, `cancelInflightBuildsForNamespace`, `startBuildFor`, `precommittedBuildFor`, `startStagingBuild`, all still present, e.g. `Parts/PartFolderAccess.cpp:297`). Several are arguably correct (they name the protocol's `inflight_builds` concept, which the spec deliberately spares). No correctness impact; optional per-method rename.
- **[build-dir-rust-localize-drift]** — KEEP (green-debt, local build-dir state) — A past cmake reconfigure can produce a `build.ninja` where every rust-contrib localize rule (chdig, polyglot, wasmtime, delta_kernel_ffi) loses its reference-library args, breaking any future `ninja` touching a rust contrib in that build dir. Fix: full cmake re-configure, and identify which configure step produced the argless state.
- **[CHANGELOG-unknown-config-key-rejection]** — KEEP, now actionable — The unknown-CAS-config-key rejection feature has shipped (`73f49694b37` "cas: ContentAddressedSettings — declarative BaseSettings table with unknown-key rejection (F4a)"), but no release-note/changelog line was found in `CHANGELOG.md` or `docs/changelogs/`. Write it. (Formerly under a "Standing hygiene" section whose other two items — dead pre-rev.6 config keys, `srid`/`server_root_id` naming — are both done: keys are gone from `src/`/`tests/`/`docs/`, `e00e0121858`; the gc-log column and `DROP POOL MEMBER` doc syntax already say `server_root_id`, `44a97ab6f89`.)

## New findings from the 2026-08-04 orphaned-open triage {#orphan-triage-2026-08-04}

- **[partpathparser-duplicated-path-constants]** — KEEP — `PartPathParser.h:25,41` still hardcodes `kDetachedDirName`/`kMovingDirName` instead of deriving them from the canonical `MergeTreeData` path constants; a future drift would silently misparse part paths.
- **[behavior-preserving-refactor-sequence]** — KEEP, partial — `CasDbg*` instrumentation is fully removed (done); event emission is already centralized (`EventEmitter`, `Primitives/CasEvent.h:93`). `RefId`/`ObjectId` typed identifiers were never introduced. Low urgency, real value.

## Bucket requirements: lifecycle / Object Lock / storage-class transitions undocumented; Glacier read unclassified (2031-triage CAS-012) {#bucket-requirements-lifecycle-worm-glacier}

Still fully unaddressed: a grep across all of `docs/en/antalya/cas/` for lifecycle expiration, Object Lock/WORM,
storage-class transitions, or Glacier finds only unrelated uses of "lifecycle" (mount/table lifecycle);
`bucket-requirements.md` documents only versioning. Add the settled position there (no lifecycle expiration,
no versioning, no Object Lock/WORM, no storage-class transitions; CAS cannot detect any of them without admin
access) — same pass as {#pool-exclusive-prefix-undocumented}. Separately: a blob transitioned to Glacier still
surfaces as a raw `S3Exception` (`isObjectNotFound`, `Backend/CasObjectStorageBackend.cpp:339`, does not
classify `InvalidObjectState`); it fails closed, so this is a diagnosability gap, not a correctness one — name
the storage-class requirement in the error instead. No restore-and-retry path is wanted.

## The pool trust boundary is nowhere stated for operators (2031-triage CAS-027) {#pool-trust-boundary-undocumented}

Still fully unaddressed: a grep for "trust boundary"/credential-trust language across `docs/en/antalya/cas/`
matches only one unrelated remark (`architecture/read-path.md:65`, about cache validation, not pool trust).
The settled position — the bucket credential IS the whole trust boundary, every party holding it is trusted
exactly as much as every other pool member (nothing in the protocol authenticates the writer of a control
object, `CasServerRoot.cpp`) — matches the code but is stated nowhere an operator will read it. Add a short
trust-boundary section (index or bucket-requirements): retiring a member, fencing writes, claiming a mount
slot are all things a pool-credential holder can do; the prefix must not be shared with an untrusted role;
backup/log-shipping/analytics roles pointed at the pool should be read-only.

## Bucket requirements never state "one pool = one bucket+prefix, no replication over it" (2031-triage CAS-032) {#pool-exclusive-prefix-undocumented}

Still fully unaddressed (0 matches for exclusive-prefix/no-replication language in `docs/en/antalya/cas/`).
Pool identity is deliberately not tied to an endpoint or bucket (`ContentAddressedExchange.h:156-158` rejects
endpoint-based identity; `CasPool.cpp:124-128` only catches a FOREIGN `pool_id`), so a cross-region-replicated
copy of the same prefix looks like the same pool. A read-only mount of such a copy fails loud or reads stale;
a WRITABLE mount of a replication destination, or bidirectional replication over the prefix, corrupts. Add
"one pool lives in exactly one bucket+prefix, nothing else writes there, no bucket replication may target it"
to `bucket-requirements.md` (same pass as {#bucket-requirements-lifecycle-worm-glacier}). Optional: record the
endpoint advisorily in the mount lease so a mismatch can be reported; identity stays `pool_id`-based.

## Condemned-displacement comments named deleted branches (2031-triage CAS-088) {#c2-displacement-comment-stale}

**✅ CLOSED by the 2026-08-23 rewrite; kept for provenance.** Re-verified: `PartWriteTxn::ensureBlobPresent`
(`Pool/CasPartWriteTxn.cpp:260`) is exactly the function now doing what the old comment described — checking
the admitted fence generation after `Condemned`, then a fresh-envelope streaming publication through
`publishBlob`. The old displacement methods and their stale comments/tests are gone.

## `resolveRef`'s `allow_stale` is an inert parameter with one stale doc comment left behind it (2031-triage CAS-110) {#resolve-ref-allow-stale-inert-parameter}

Still fully open, unchanged in substance (line numbers drifted ~10 lines). `CasRefLedger::resolveRef` names
the parameter only in a comment (`Pool/CasRefLedger.cpp:285`, `bool /*allow_stale*/`); both declarations
(`CasRefLedger.h:138`, `CasPool.h:588`) and the two remaining call sites (`Parts/PartFolderAccess.cpp:289,579`)
still forward it. Intended: with the snapshot+log protocol there is one authoritative cached `RefTableState`
per mounted writer, no second per-shard decode cache to be stale against. Owed: drop the parameter from both
declarations and both call sites, and fix the still-wrong doc comment at `PartFolderAccess.h:61`
(`CachedForLoad`, "stale-tolerant resolve (allow_stale=true)"). `Freshness` itself stays load-bearing
(`ForceFresh`/`StrictValidate` still gate `getView`'s manifest-body proof). P3.

## Public docs: unshipped artifacts, wrong CLI names (umbrella review M12) {#public-docs-accuracy-m12}

Two of three sub-claims remain open (the settings-coverage one, 12b, shipped: `a923b9888b6` "docs: complete
CAS config-key namespace coverage" — every `ContentAddressedSettings` entry is now documented, verified by
diffing all 31 `DECLARE(...)` names against `configuration.md`'s table).

- **12a** — `correctness.md:21,25` still cites `docs/superpowers/models/`, a development-branch-only path,
  as the "how CAS safety was verified" evidence. Rewrite to cite only what ships, or ship a public subset.
- **12c** — wrong `ca-*` prefix instead of the registered `cas-*` for `clickhouse-disks` commands. Now
  **10 occurrences across 7 pages** (was "nine across six"): `blob-protocol.md`, `correctness.md`,
  `read-path.md`, `replication.md`, `garbage-collection.md` (×3), `roadmap.md`, `manifests-and-refs.md` (×2).

P2, and cheap: one docs pass. Nothing here is tracked elsewhere (`deferred-docs-fixes.md` is still empty).

### `[system-md-missing-cas-verbs]` ✅ CLOSED by `ada2908ac7a` (`cas-gc-rebuild` only) — `SYSTEM CAS` verbs missing from `docs/en/sql-reference/statements/system.md`

DOC — Found during Task 12 (operations runbooks): `SYSTEM CAS FSCK`/`FORGET`/`GC STOP`/`GC START` (and
siblings) are documented in the CAS-specific pages but absent from the generic `SYSTEM` statement
reference, where a user would naturally look first. `docs/en/sql-reference/statements/system.md` now
documents every `SYSTEM CAS` verb.

## Retry-later class has no ProfileEvent (umbrella review M13) {#retry-later-no-profile-event}

Still fully open. `throwCasWriteRetryLater` (`Backend/CasRequests.cpp:92-96`) is reached from ~60 call sites
(grep count; review said "40+"/"63") and still only calls `logCasWriteRetryLater`, a `LogSeriesLimiter`-gated
`LOG_WARNING` (`:85-89`) — no `ProfileEvents::increment` anywhere in either function, so write-contention
cannot be trended or alerted on. Add an aggregate ProfileEvent at the throw. The sibling gap — no signal
distinguishing "GC administratively stopped" from "not GC leader" — is tracked as {#gc-health-zero-is-ambiguous}
(2031-triage CAS-098). P2.
