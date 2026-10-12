---
description: TLC evidence for the root-local part-manifest GC core, and the verdict on the retired token-diff discovery skip.
sidebar_label: Root-local part-manifest GC results
sidebar_position: 1
slug: /superpowers/models/ca-gc-root-local-part-manifest-core-results
title: CaGcRootLocalPartManifestCore — TLA+ gate results
doc_type: reference
---

# `CaGcRootLocalPartManifestCore` — TLA+ gate results {#ca-gc-root-local-part-manifest-core-results}

The model covers the root-local part-manifest GC core: single-owner
`ManifestId`s with owner transitions and blob-only in-degree, precommit
lifecycle, missing-body handling, orphan-manifest sweep, mutable-payload
updates, lazy trim, target-sharded reducers, the retire-token optimization,
and attempt-scoping. This is the first `_RESULTS.md` for this model in the
current doc set.

## Verdict: the `listedTok` skip premise is retired {#verdict-listedtok-skip-premise-retired}

`CanSkipShard(n)` gates the Phase 2 token-diff discovery skip on
`listedTok[n] = foldedTok[n]` — a claim that a root shard's fold coverage is
current because a LIST-observed per-shard token matches the persisted
`ShardCoverage.folded_token`, letting `GDiscoverSkip` advance the fold cursor
to the journal end **without reading the shard's body**. The question this
task closes: does any production code path still legitimately consume a
LIST-derived token as authority to skip that read?

**No.** Searched for the production seam this model corresponds to
(`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/`) and
found no `ShardCoverage`/`folded_token` type, and no discovery path that
elides a fold read on a listing match:

- `supportsListTokens()`/`tokenForList` (`CasObjectStorageBackend.h`,
  `CasObjectStorageBackend.cpp`) surface a LIST-derived incarnation token, but
  only for the blob-condemn exact-delete-vs-`HEAD`-first decision on an
  already-condemned object — never to skip a manifest/owner-transition fold
  read.
- The landed ref-log intake fold (`CasGc.cpp`, the frozen-tail walk around the
  `intake_tails_advanced`/`intake_tails_unchanged`/`intake_tails_below_cursor`
  classification) has the closest matching shape — `tail` is the round-start
  LIST's greatest listed id per namespace, `cursor` is
  `RefCoverage.last_folded_ref_id`, and `cursor == tail` is exactly this
  model's `listedTok[n] = foldedTok[n]` condition. But that design explicitly
  does **not** skip the read when the bucket is `tail == cursor`: the walk
  still issues the single exact-key `GET` at `cursor + 1`, because — per the
  comment there — "that read IS the frontier proof: an absent expected-next
  with no witness above it is the ONLY thing anywhere that sets
  `frontier_proven`... So `skip reading a quiet namespace` is not a cheaper
  version of this design; it is a GC that permanently reclaims nothing."

So the landed architecture considered exactly this shape of optimization and
rejected it — not only for the model's `INV_NO_DANGLE` reason
(`SabotageSkipChangedShard` proves that half), but for a liveness reason this
model does not represent: the read is the only thing that ever proves a
namespace's frontier, and a hot LIST is untrusted for that proof. Universe
membership comes from the catalog, and recovery is LIST-independent
(`chooseRecoveryGrounding`, `_ckpt.committed_through`); a listing may only
ever offer a newer candidate, a diagnostic, or a garbage nomination, never a
correctness decision — and `GDiscoverSkip` using `listedTok` to license
skipping a read is precisely a correctness decision resting on LIST fidelity.

**Action taken: retire, not rewrite.** The premise cannot be rescued by
tightening a guard — the landed design's answer to "the shard looks
unchanged" is "read it anyway; the read is the proof," which is a different
shape of solution than a listing-gated skip, not a stricter version of it.
Retired:

- The four configs that exercised `EnableTokenDiff = TRUE` are removed:
  `stage5_tokendiff`, `sab_skipchangedshard`, `sab_skipparksdeadprecommit`,
  `fix_skipparksdeadprecommit`. No other config in this model ever set
  `EnableTokenDiff = TRUE`, so removing these four is the complete retirement
  — every remaining config's state space is unaffected (all forty-two others
  already ran with `EnableTokenDiff = FALSE`).
- `CaGcRootLocalPartManifestCore.tla` gained two comments (at the `CONSTANTS`
  declaration and at the Phase 2 action block) marking `EnableTokenDiff`,
  `TokenObservable`, `GDiscoverSkip`, `GDiscoverRead`, and the two `Sabotage*`
  controls in that arm as retired: no active config exercises them, and they
  are kept only as a historical negative-space record of an explored and
  rejected design, not as validation of an adoptable optimization. No
  transition, invariant, or other action changed, so no TLC rerun of the
  Phase 2 machinery is required by this change alone; the whole-suite rerun
  in the ninth-family battery below (which must exercise the post-verdict
  config set) is the standing evidence pass.

## Battery: the ninth family {#battery-the-ninth-family}

`run_gc_partmanifest.sh` asserts every config's exact outcome kind and name in
one script: 39 fast rows plus 5 `SLOW=1` rows (44 total, after Step A1 retired
the four `EnableTokenDiff = TRUE` configs from what was previously 43 fast
rows). Checker identity: `TLC2 Version 2026.07.18.145032 (rev: 30cc360)`,
jar SHA-256 `cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3`
(the pinned official jar, `check_tlc_pin` refuses any other). All runs
`-workers 1`, deterministic breadth-first search.

### First pass: two timeouts {#first-pass-two-timeouts}

The first whole-suite run (`SLOW=1`, default `TLC_TIMEOUT=3600`) resolved 46
of 48 rows to their expected outcome. Two `green`-expected rows hit the
3600s/3601s wall-clock bound without a verdict: `stage5_lazytrim` and `live`.
A TLC timeout is a resource bound, not a model verdict, so neither was
recorded as a violation, a pass, or a downgraded expectation — both were
re-run standalone with `TLC_TIMEOUT=14400` (4h), `-workers 1` unchanged (no
prior record for this model pinned worker count as part of a row's identity,
but every other row in this same battery ran at `-workers 1`, and changing
only these two would make their state-space exploration order incomparable
to the rest of the table, so the extra time is spent at the same
determinism instead of added parallelism).

### Exact runner tail (first pass) {#exact-runner-tail-first-pass}

```text
CONFIG                             EXPECT            RESULT                                 SECONDS  VERDICT
sab_acceptnamespacemismatch        violation         violation:INV_NO_DANGLE                45       PASS
sab_acceptrefmismatch              violation         violation:INV_NO_LOSS                  1        PASS
sab_advancepastmissingbody         violation         violation:INV_NO_DANGLE                19       PASS
sab_barenonce                      violation         violation:INV_NO_LOSS                  2        PASS
sab_commitskipblobreval            violation         violation:INV_NO_DANGLE                0        PASS
sab_crosssharddisplacement         known-model-error model-error:UnchangedCompositeVars     1        KNOWN
sab_cutoverclaim                   violation         violation:INV_NO_DANGLE                6        PASS
sab_deletebodybeforedecrements     temporal          temporal:NoLeakForever                 1341     PASS
sab_deposedleaderwritesfinalgen    violation         violation:INV_ONLY_ADOPTED_VIEWABLE    0        PASS
sab_frozenseqauthority             violation         violation:INV_NO_DANGLE                1        PASS
sab_keybyrefnotid                  violation         violation:INV_NO_LOSS                  252      PASS
sab_lazyfenceunsafe                violation         violation:INV_NO_DANGLE                831      PASS
sab_missingbodyactivated           violation         violation:INV_NO_LOSS                  20       PASS
sab_missingcommittedempty          violation         violation:INV_NO_LOSS                  1        PASS
sab_mutableasreachability          violation         violation:INV_NO_LOSS                  3        PASS
sab_nofence                        violation         violation:INV_NO_DANGLE                1        PASS
sab_noorphansweep                  temporal          temporal:OrphanManifestDebrisDrains    192      PASS
sab_precommitlessprotect           violation         violation:INV_NO_DANGLE                1        PASS
sab_promoteaftermissingbody        violation         violation:INV_NO_LOSS                  0        PASS
sab_reducerownsfence               known-model-error model-error:UnchangedCompositeVars     0        KNOWN
sab_reusedtag                      violation         violation:INV_NO_RETURN                1        PASS
sab_reusemanifestid                violation         violation:INV_NO_LOSS                  24       PASS
sab_roundvisibilityearly           violation         violation:INV_NO_DANGLE                1        PASS
sab_splitpromote                   violation         violation:INV_NO_DANGLE                0        PASS
sab_staletokenoverdelete           violation         violation:INV_NO_LOSS                  1        PASS
sab_trimunincorporated             violation         violation:INV_JOURNAL_COVERAGE         0        PASS
sab_twoowners                      violation         violation:INV_NO_LOSS                  14       PASS
sab_unconddelete                   violation         violation:INV_NO_DANGLE                3        PASS
sab_wholesaleprefixdelete          violation         violation:INV_NO_DANGLE                0        PASS
empty_namespaces                   green             green                                  1        PASS
stage0                             green             green                                  1        PASS
stage1                             green             green                                  14       PASS
stage5_retiretoken                 green             green                                  123      PASS
stage5_sharding                    known-model-error model-error:UnchangedCompositeVars     1        KNOWN
stage6_attemptscoping              green             green                                  447      PASS
witness_committedoverfoldedblob    violation         violation:W_CommittedOverFoldedBlob    1        PASS
witness_orphandeleted              violation         violation:W_OrphanDeleted              0        PASS
witness_precommitmissingbody       violation         violation:W_PrecommitMissingBodyReached 0        PASS
witness_twoleadersoneadopt         violation         violation:W_TwoLeadersOneAdopt         1        PASS
stage2                             green             green                                  2934     PASS
stage3                             incomplete        timeout                                60       KNOWN
stage4                             green             green                                  1154     PASS
stage5_lazytrim                    green             timeout                                3601     FAIL
live                                green             timeout                                3600     FAIL

SOME EXPECTATIONS UNMET
```

### Per-configuration evidence {#per-configuration-evidence}

Generated / distinct state counts and search depth, from each config's own
TLC log (first-pass run; `stage3`'s expectation is `incomplete` by design —
its `TLC_SLOW_TIMEOUT=60` timeout IS the expected outcome, so it is not
re-run at a larger bound).

| Config | Outcome | Generated / distinct | Depth |
| --- | --- | ---: | ---: |
| `sab_acceptnamespacemismatch` | `violation:INV_NO_DANGLE` | 5,347,402 / 1,232,755 | 12 |
| `sab_acceptrefmismatch` | `violation:INV_NO_LOSS` | 89,119 / 29,290 | 11 |
| `sab_advancepastmissingbody` | `violation:INV_NO_DANGLE` | 2,255,308 / 530,447 | 13 |
| `sab_barenonce` | `violation:INV_NO_LOSS` | 119,623 / 39,583 | 11 |
| `sab_commitskipblobreval` | `violation:INV_NO_DANGLE` | 8 / 7 | 2 |
| `sab_crosssharddisplacement` | `model-error:UnchangedCompositeVars` (KNOWN) | 11 / 10 | 2 |
| `sab_cutoverclaim` | `violation:INV_NO_DANGLE` | 637,765 / 218,880 | 13 |
| `sab_deletebodybeforedecrements` | `temporal:NoLeakForever` | 33,805,258 / 9,295,903 | 23 |
| `sab_deposedleaderwritesfinalgen` | `violation:INV_ONLY_ADOPTED_VIEWABLE` | 1,316 / 482 | 5 |
| `sab_frozenseqauthority` | `violation:INV_NO_DANGLE` | 942 / 345 | 5 |
| `sab_keybyrefnotid` | `violation:INV_NO_LOSS` | 27,770,582 / 7,096,048 | 17 |
| `sab_lazyfenceunsafe` | `violation:INV_NO_DANGLE` | 120,837,524 / 16,969,414 | 11 |
| `sab_missingbodyactivated` | `violation:INV_NO_LOSS` | 2,255,648 / 530,675 | 13 |
| `sab_missingcommittedempty` | `violation:INV_NO_LOSS` | 125,527 / 41,678 | 11 |
| `sab_mutableasreachability` | `violation:INV_NO_LOSS` | 256,932 / 82,651 | 12 |
| `sab_nofence` | `violation:INV_NO_DANGLE` | 52,895 / 16,678 | 10 |
| `sab_noorphansweep` | `temporal:OrphanManifestDebrisDrains` | 3,668,037 / 1,043,212 | 16 |
| `sab_precommitlessprotect` | `violation:INV_NO_DANGLE` | 122,015 / 39,349 | 11 |
| `sab_promoteaftermissingbody` | `violation:INV_NO_LOSS` | 172 / 82 | 4 |
| `sab_reducerownsfence` | `model-error:UnchangedCompositeVars` (KNOWN) | 12 / 11 | 2 |
| `sab_reusedtag` | `violation:INV_NO_RETURN` | 40,169 / 13,100 | 9 |
| `sab_reusemanifestid` | `violation:INV_NO_LOSS` | 2,815,250 / 697,873 | 16 |
| `sab_roundvisibilityearly` | `violation:INV_NO_DANGLE` | 63,286 / 21,641 | 10 |
| `sab_splitpromote` | `violation:INV_NO_DANGLE` | 166 / 76 | 4 |
| `sab_staletokenoverdelete` | `violation:INV_NO_LOSS` | 43,845 / 14,196 | 16 |
| `sab_trimunincorporated` | `violation:INV_JOURNAL_COVERAGE` | 86 / 45 | 4 |
| `sab_twoowners` | `violation:INV_NO_LOSS` | 1,457,218 / 424,630 | 14 |
| `sab_unconddelete` | `violation:INV_NO_DANGLE` | 239,356 / 77,008 | 12 |
| `sab_wholesaleprefixdelete` | `violation:INV_NO_DANGLE` | 868 / 352 | 5 |
| `empty_namespaces` | `green` | 2,708 / 696 | 14 |
| `stage0` | `green` | 71,184 / 19,846 | 32 |
| `stage1` | `green` | 1,659,466 / 402,034 | 35 |
| `stage5_retiretoken` | `green` | 17,565,591 / 3,529,248 | 34 |
| `stage5_sharding` | `model-error:UnchangedCompositeVars` (KNOWN) | 12 / 11 | 2 |
| `stage6_attemptscoping` | `green` | 70,638,390 / 11,658,986 | 41 |
| `witness_committedoverfoldedblob` | `violation:W_CommittedOverFoldedBlob` | 326 / 139 | 5 |
| `witness_orphandeleted` | `violation:W_OrphanDeleted` | 118 / 58 | 4 |
| `witness_precommitmissingbody` | `violation:W_PrecommitMissingBodyReached` | 8 / 7 | 2 |
| `witness_twoleadersoneadopt` | `violation:W_TwoLeadersOneAdopt` | 334 / 154 | 4 |
| `stage2` | `green` | 377,632,669 / 68,550,326 | 50 |
| `stage3` | `timeout` (KNOWN — `incomplete` by design, `TLC_SLOW_TIMEOUT=60`) | n/a | n/a |
| `stage4` | `green` | 134,769,744 / 27,396,110 | 42 |

`stage5_lazytrim` and `live` — see the extended-bound rerun below.

### Extended-bound rerun: `stage5_lazytrim` and `live` {#extended-bound-rerun}

Both re-run standalone (`TLC_TIMEOUT=14400`, 4h bound, `-workers 1`
unchanged) once the first pass's other 46 rows had already resolved.

**`live` — GREEN.** Completed in 2h05m (7536s): `74,147,107` states
generated, `17,845,340` distinct, depth 38, "No error has been found." This
reproduces the exact state counts on record from before the
`docs/superpowers` consolidation, confirming the extended bound changed
nothing about the model, only the time available to it.

**`stage5_lazytrim` — UNPROVEN-BY-TIMEOUT.** Still times out at the
extended 4h bound (`14401`s). At cutoff: `1,333,723,653` states generated,
`233,198,128` distinct, depth 31, `24,395,446` states still left on the
queue and draining only slowly (the closing one-minute samples each removed
on the order of tens of thousands off the queue). This is booked as a
second named model debt, not a fixed row and not a downgraded expectation:
the config's positive claim (`INV_NO_DANGLE`/`INV_NO_LOSS`/etc. hold under
`EnableLazyTrim = TRUE` across a bounded cross-namespace shared-blob
scenario) remains unproven by TLC on this hardware at `-workers 1`. The
queue was not close to draining at the 4h mark, so more wall-clock time
alone is not a promising next step; closing this debt would need either a
tighter `StateConstraint` for this specific row (trading scenario coverage
for feasibility) or a checker run with more workers/memory, neither of
which was attempted here in order to keep this row directly comparable in
determinism to the rest of the table.

## Standing model debt {#standing-model-debt}

**`stage5_lazytrim` is UNPROVEN-BY-TIMEOUT** — see the extended-bound
rerun immediately above: the row's positive safety claim under
`EnableLazyTrim = TRUE` has not been checked to completion on this
hardware, at a 4h/`-workers 1` bound reaching `1,333,723,653` generated /
`233,198,128` distinct states with `24,395,446` still queued.

**The Phase-4 sharding arm is UNPROVEN.** Three configs —
`sab_crosssharddisplacement`, `sab_reducerownsfence`, `stage5_sharding` — all
resolve to `known-model-error:UnchangedCompositeVars`: TLC rejects
`GReduceShard`'s composite `UNCHANGED vars` tuple as referencing an
undefined identifier rather than exploring the intended counterexample.
`origVars`, `GReduceShard`'s `UNCHANGED vars` line, and the runner's own
provenance check (`orig_vars_line`/`reduce_unchanged_line` in
`run_gc_partmanifest.sh`) all confirm this is the model's own composite-var
regression, not an environment fault — accepting an arbitrary TLC error here
would turn a broken model into false red evidence, so these three configs
are asserted to hit exactly that error, not silently skipped. This means
`EnableSharding`'s cross-shard displacement guard and the reducer-owns-fence
guard are **not exercised by TLC** — a standing model debt carried forward,
not fixed by this task.

## CAS-36: rowless-namespace nomination in the orphan sweep {#cas-36-rowless-namespace-nomination}

The orphan sweep deletes a manifest body that a committed ref names. The sweep nominates a listed body
with no floor and no owner check when its namespace has no catalog row. A writer stages the body before
the row exists, then births the row, precommits and promotes. The GC deletes the body after its
`gc/state` CAS. Result: a committed ref over a missing manifest (`INV_NO_DANGLE`).

Code at `d76b59f9295`:

- `Gc/CasOrphanManifestSweep.cpp` `planManifestCursorPage`: LIST, then one catalog cut, then per
  namespace the floor (`floorForNamespace`, only `if (catalog_entry)`) and the owner view. A rowless
  key goes straight to `candidates`.
- `Gc/CasGc.cpp` PHASE 18 `orphan_sweep`: HEAD + exact-etag `remove` after the CAS. No re-check.
- The row is born only in `PartWriteTxn::precommitAdd` (`appendRefOps` →
  `CasRefLedger::acquireMutableRefTableRuntime` → `CasRefLedger::namespaceLife`), after
  `PartWriteTxn::stageManifest`.

### Can `to_ns` be unborn when `stageManifest` runs? {#can-to-ns-be-unborn}

Yes, on both paths.

- **Table move**: `ContentAddressedTransaction::moveDirectory` (table→table branch) calls
  `CachedPartFolderAccess::republishRef` for every ref first. It goes `publishEntries` →
  `prepareEntries` → `stageManifest` → `precommitAdd`. `namespaceLife(to_ns)` runs afterwards, and only
  if the source has namespace files. The first republished ref stages into an unborn `to_ns`.
- **Ordinary INSERT**: `ContentAddressedTransaction::publishStaging` calls `stageManifest` then
  `precommitAdd`, with no `namespaceLife` before. The namespace is usually born at `CREATE TABLE`:
  `MergeTreeData`'s constructor writes `format_version.txt`, and `ContentAddressedTransaction::writeFile`
  resolves `namespaceLife` for it. But the constructor writes that file only to the first writable disk
  of the storage policy. A CAS disk that is not first (for example local hot volume, CAS cold volume)
  has no row until its first part lands. That first write stages into an unborn namespace.
- **Rebirth**: `CasRefLedger::namespaceLife` rebirths a namespace whose row was removed. Nothing pins the
  life a body was staged under, so a body staged under life 1 can be precommitted under life 2.
  Table-level locks in the engine normally keep `DROP TABLE` away from an in-flight INSERT; the CAS
  layer itself does not.

### What was split {#cas-36-what-was-split}

`GOrphanSweep` observed the owner and deleted in one step. Under `EnableCatalogRows = TRUE` it is
replaced by one action per observation, in code order:

| Action | Code |
| --- | --- |
| `GSwList` | `op.list(casManifestsPrefix)` |
| `GSwCatalog` | `CasRefCatalog::read` (rowless namespaces) |
| `GSwFloor` | `floorForNamespace` → `prefixEligibleUnder` |
| `GSwView` | `activeManifestKeys` (owner view of catalog-named namespaces) |
| `GSwNominate` | body GETs (`PersistedEtag::capture`) + the `gc/state` CAS |
| `GSwDelete(m)` | post-CAS HEAD + `remove`, no re-check |

Writer side under the same flag:

- `WCreateNamespace(n)`: `CasRefLedger::namespaceLife` births the row.
- `WRemoveNamespace(n)` (`EnableNsRemoval`): `dropNamespace` + terminal catalog removal, one step.
- `WEndBuild(p)`: the build leaves the active set and the lease renewal publishes
  `min_active_build_sequence` past it. After it, no request of that build runs (`BuildActive`).
- Precommit/promote/repoint need the row (`RowGate`). `WPublishCommitted` is disabled: the code always
  commits through `precommitAdd` + `promote`.

Every legacy config sets `EnableCatalogRows = FALSE`, so its reachable space is unchanged.

### Candidates {#cas-36-candidates}

| Constant | Candidate |
| --- | --- |
| `FixStageAfterBirth` (A) | `WStageManifest` needs the row (call `namespaceLife` before `stageManifest`) |
| `FixPinLife` | owner transitions refuse a life other than the one the body was staged under |
| `FixFloorAll` (A') | the floor applies to every listed key, row or not (CAS-36's wording) |
| `FixFloorBeforeCatalog` (A'') | the floor is read before the catalog cut |
| `FixIfMatch` (C) | the delete is conditional on the token recorded at the body GET |

### Results {#cas-36-results}

TLC `-workers 4`, `-Xmx8g`, `nice 10`. One namespace, one manifest, one blob, one build.

| Config | Fixes | Outcome | Generated / distinct | Depth or trace | Wall |
| --- | --- | --- | ---: | ---: | ---: |
| `cur_rowless` | none | `INV_NO_DANGLE` violated | 80,247 / 25,583 | 11 states | 2 s |
| `fix_stageafterbirth` | A + pin, removal on | green | 75,266,756 / 13,934,178 | 50 | 152 s |
| `fix_stageafterbirth_norebirth` | A, removal off | green | 12,410,102 / 2,486,379 | 47 | 30 s |
| `sab_stageafterbirth_nopin` | A, no pin, removal on | `INV_NO_DANGLE` violated | 412,897 / 122,962 | 14 states | 3 s |
| `fix_floorall` | A' | `INV_NO_DANGLE` violated | 202,104 / 61,753 | 12 states | 1 s |
| `fix_floorfirst` | A' + A'' | green | 129,056,456 / 23,766,088 | 50 | 264 s |
| `sab_floorfirst` | A'' without A' | `INV_NO_DANGLE` violated | 104,244 / 32,834 | 11 states | 1 s |
| `fix_ifmatch` | C | `INV_NO_DANGLE` violated | 98,707 / 30,982 | 12 states | 2 s |
| `fix_combined` | A + pin + A' + A'' | green | 66,909,605 / 12,374,824 | 50 | 132 s |
| `witness_rowlessswept_floorfirst` | A' + A'' | `W_RowlessDebrisSwept` reached | 21,001 / 6,981 | 9 states | 1 s |
| `witness_rowlessswept_stageafterbirth` | A + pin | `W_RowlessDebrisSwept` reached | 46,792 / 15,293 | 11 states | 1 s |

The two witnesses show the green fixes still delete debris of a rowless namespace, so they are not
green by deleting nothing.

### Legacy battery after the change {#cas-36-legacy-battery}

The fast battery (39 legacy rows, `SLOW=1` rows not run) keeps every recorded outcome at `-workers 4`.
Distinct-state counts of the green rows are unchanged: `empty_namespaces` 696, `stage0` 19,846,
`stage1` 402,034, `stage5_retiretoken` 3,529,248, `stage6_attemptscoping` 11,658,986. Longest rows:
`sab_deletebodybeforedecrements` 629 s, `sab_lazyfenceunsafe` 515 s, `stage6_attemptscoping` 203 s.
Legacy rows still use the fused `GOrphanSweep`; the split applies only under `EnableCatalogRows`.

### Counterexamples {#cas-36-counterexamples}

**Current protocol (`cur_rowless`)**:

1. `WStageManifest(m1)`: the writer PUTs the body; `n1` has no row.
2. `GSwList`: GC lists the body.
3. `GSwCatalog`: GC reads the catalog; `n1` is rowless.
4. `WCreateNamespace(n1)`, `WPrecommitAdd(m1)`, `WPromote(m1)`: the promote re-reads the body (present)
   and commits `r1 → m1`.
5. `GSwFloor`, `GSwView`, `GSwNominate`: rowless, so no floor and no owner check; `m1` is nominated.
6. `GSwDelete(m1)`: the body is deleted under the committed ref.

**A' alone (`fix_floorall`)**: same, but the build ends (`WEndBuild`) after the promote and before
`GSwFloor`. The floor read now says "eligible", and the rowless cut taken earlier skips the owner view.
The floor proves the build ended; it proves nothing about owners unless the catalog cut is younger
than the floor read.

**A without the life pin (`sab_stageafterbirth_nopin`)**: row born, body staged, row removed (no owners
yet), GC lists and cuts (rowless), the writer's `precommitAdd` rebirths `n1` and promotes, GC deletes.

**C (`fix_ifmatch`)**: same as the current trace. The manifest key is write-once and the writer never
re-PUTs it, so the token GC records is the token of the body the ref commits. The base code already
deletes orphan nominations by HEAD + exact etag (`CasGc.cpp` PHASE 18); the owner-removed path
(PHASE 15, `removeCohortWriteOnce`) has no token, but its bodies cannot be re-owned.

### Verdict {#cas-36-verdict}

- **A' + A'' (floor on every key, read before the catalog cut)**: sufficient alone. If the floor read
  says a build ended, every owner transition of that build happened before the read. Those need a row,
  so a later catalog cut shows the row, or the row was removed after all owners left. Recommended.
- **A (stage after birth)**: sufficient only with the life pin. Without the pin, removal + rebirth
  reopens the race. The code has no pin today: `precommitAdd` resolves the life again.
- **A' alone**: not sufficient (the floor is read after the catalog cut).
- **C**: not sufficient; no effect on this race.

Code cost of A' + A'': in `planManifestCursorPage`, read `floorForNamespace` for every namespace of the
page before `CasRefCatalog::read`, and drop the `if (catalog_entry)` around `prefixEligibleUnder`. One
mount-lease GET per distinct namespace per page; rowless namespaces add one GET each (the CAS-36 cost).
No writer change.

The nomination's blob source retirement (`orphan_source_retirements`) is modeled in the residual
section below. CAS-310 (owner-removal invariant for the sweep) is still open and was not added here.

### Residual: late requests of a fenced or superseded epoch {#cas-36-residual-late-requests}

A'' + A' stays green only while every request of an epoch lands before GC reads a floor that retires
that epoch. The model now drops that assumption and shows what holds it.

**What changed in the model** (all inert unless `EnableFenceEpochs`):

| Action / constant | Code at `d76b59f9295` |
| --- | --- |
| `WAdmitOp` | `CasRequests::admit` → `Fence::admit`: an operation is admitted under the live lease of `mountEpoch` |
| `WResolveOp` | the operation's last request returned or failed |
| `GFence` | `computeHeartbeatFloor` fence-out `readModifyWrite`: `gc_fenced = true`, body kept |
| `WRemount` | `claimMountAwaitingExpiry`: `writer_epoch + 1`; old-epoch builds keep their ended state (resetting it revived an ended build: a modeling error caught by `superseded_late_bound`) |
| `OpIssue(m)` | stage, `namespaceLife`, `precommitAdd`, promote, repoint need an operation admitted under m's epoch; they are not re-checked against the fence |
| `GcFencedEligible` | FALSE = base `prefixEligibleUnder` (ignores `gc_fenced`); TRUE = proposed AC #6 (a fenced epoch's builds count as retired) |
| `AdmissionBound` | TRUE = `GSwFloor` cannot report an epoch retired while an operation admitted under it is unresolved; FALSE = no bound |
| `mEpoch[m]` | `writer_epoch` baked into the manifest key at staging |

`BuildActive` stays for the `WEndBuild` path: within one epoch a build never retires with an unresolved
`precommitAdd`/promote (the retire sites wait for the append or the cleanup-duty drain), so `WEndBuild`
now also requires no in-flight operation and a live lease.

**Blob protection** (`GSwNominate`, all `EnableCatalogRows` configs): the nomination's `gc/state` CAS now
folds `orphan_source_retirements` (`CasGc.cpp` PHASE 18 → `foldDeltasIntoGeneration`): every blob source
edge `(m, path)` of a nominated body is retired. A later activation of m re-adds the edge; a promote
(owner move) does not. New invariant `INV_COMMITTED_BLOBS_PRESENT`: every blob a committed manifest names
is present, whether or not its body is.

### Residual results {#cas-36-residual-results}

TLC `-workers 4`, `-Xmx8g`, `nice 10`. The epoch configs use `fix_floorfirst`'s fixes (A' + A''), with
`EnableNsRemoval = FALSE` and `MaxToken = 1` so a run ends inside the 20-minute limit.

| Config | Setting | Outcome | Generated / distinct | Depth or trace | Wall |
| --- | --- | --- | ---: | ---: | ---: |
| `cur_rowless_blobloss` | current protocol, only `INV_COMMITTED_BLOBS_PRESENT` | violated | 3,071,584 / 788,099 | 19 states | 8 s |
| `fenced_late_bound` | `GFence`, `GcFencedEligible`, `AdmissionBound` | green | 39,686,813 / 7,047,428 | 43 | 86 s |
| `fenced_late_unbounded` | same, no `AdmissionBound` | `INV_NO_DANGLE` violated | 488,919 / 136,870 | 13 states | 3 s |
| `superseded_late_bound` | `WRemount`, `GcFencedEligible = FALSE`, `AdmissionBound` | green | 404,543,956 / 69,594,492 | 45 | 899 s |
| `superseded_late_unbounded` | same, no `AdmissionBound` | `INV_NO_DANGLE` violated | 1,803,061 / 506,911 | 14 states | 7 s |
| `witness_fencedswept` | as `fenced_late_bound` | `W_FencedDebrisSwept` reached | 62,765 / 19,678 | 11 states | 1 s |

The CAS-36 rows were rerun with the source retirement in `GSwNominate` and `INV_COMMITTED_BLOBS_PRESENT`
added to every green fix. Every outcome holds:

| Config | Outcome | Generated / distinct | Wall |
| --- | --- | ---: | ---: |
| `cur_rowless` | `INV_NO_DANGLE` violated (12 states) | 66,152 / 21,085 | 2 s |
| `fix_stageafterbirth` | green | 75,278,812 / 13,936,236 | 387 s |
| `fix_stageafterbirth_norebirth` | green | 12,416,130 / 2,487,408 | 98 s |
| `sab_stageafterbirth_nopin` | `INV_NO_DANGLE` violated | 389,612 / 115,996 | 8 s |
| `fix_floorall` | `INV_NO_DANGLE` violated | 232,281 / 70,869 | 6 s |
| `fix_floorfirst` | green | 129,086,596 / 23,771,233 | 441 s |
| `sab_floorfirst` | `INV_NO_DANGLE` violated | 95,029 / 30,042 | 1 s |
| `fix_ifmatch` | `INV_NO_DANGLE` violated | 98,790 / 31,012 | 1 s |
| `fix_combined` | green | 66,921,661 / 12,376,882 | 135 s |
| `witness_rowlessswept_floorfirst` | reached | 21,038 / 7,002 | 0 s |
| `witness_rowlessswept_stageafterbirth` | reached | 46,788 / 15,486 | 2 s |

Green counts grew slightly (for example `fix_floorfirst` 23,766,088 → 23,771,233): the retirement adds
states where a nominated edge is gone before the body delete. Wall times differ from the first table
because other TLC runs shared the host. `superseded_late_bound` uses 899 s of the 1200 s limit; do not
widen its constants.

The 39 legacy fast rows keep every recorded outcome, and the green rows keep their distinct counts
(`empty_namespaces` 696, `stage0` 19,846, `stage1` 402,034, `stage5_retiretoken` 3,529,248,
`stage6_attemptscoping` 11,658,986): the new actions and variables are inert without `EnableCatalogRows`
and `EnableFenceEpochs`.

### Residual counterexamples {#cas-36-residual-counterexamples}

**`fenced_late_unbounded`** (13 states):

1. `WAdmitOp`: an INSERT is admitted under epoch 1.
2. `WStageManifest(m1)`, `WCreateNamespace(n1)`: body staged, row born.
3. `GSwList` lists `m1`. `GFence`: GC fences out epoch 1. The operation keeps running.
4. `GSwFloor` (epoch 1 fenced → retired), `GSwCatalog` (`n1` has a row), `GSwView` (`m1` has no owner).
5. `WPrecommitAdd(m1)`, `WPromote(m1)` land late: the promote reads the body (present) and commits `r1 → m1`.
6. `GSwNominate`: the floor says ended and the view says unowned → `m1` nominated.
7. `GSwDelete(m1)`: committed ref over a missing body.

**`superseded_late_unbounded`** (14 states): same shape, plus an idle `GStartRound`. `WRemount` replaces `GFence`; the floor reports
epoch 1 retired because `writer_epoch` 1 < 2 (`prefixEligibleUnder`). `GcFencedEligible = FALSE`.

**`cur_rowless_blobloss`** (19 states, current protocol, only the blob invariant checked): `m1` staged
rowless, `b1` uploaded and retired in round 1 at in-degree 0; GC lists and cuts (rowless); the writer
births the row, precommits and promotes (its view predates the round-1 retire). The fold adds the
precommit's edge (`b1` in-degree 1). `GSwNominate` retires that edge (in-degree 0); the recheck no longer
spares `b1`, the delete lands. A committed ref names a deleted blob, independently of the body delete.

### Residual verdict {#cas-36-residual-verdict}

- **The residual is not limited to rowless namespaces.** The shortest trace above has a catalog row. The
  owner view (`activeManifestKeys`) is one read; an owner transition that lands after it is invisible to
  the nomination. The A'' argument ("every owner transition of an ended build happened before the floor
  read") is exactly the admission bound.
- **Excluding `gc_fenced` does not remove the time dependency.** `superseded_late_unbounded` violates with
  `GcFencedEligible = FALSE`. At the base this is not only a zombie case: a self-remount or restart
  publishes a higher `writer_epoch`, `prefixEligibleUnder` then makes every build of the old epoch
  eligible, and the old epoch's in-flight `precommitAdd`/promote is fenced only by an `EpochSeal` that
  recovery writes lazily per namespace on next touch, with no grace wait (`CasPool.cpp` "NO WAIT HERE").
  A namespace the successor never touches (for example a rowless one) never gets that seal. `EpochSeal`
  is not modeled.
- **With `AdmissionBound` both arms are green** (`fenced_late_bound`, `superseded_late_bound`). The
  witness `witness_fencedswept` shows the fenced arm still deletes debris whose build never ended through
  `WEndBuild`.
- **Blob loss**: the current protocol also loses a committed manifest's blob through
  `orphan_source_retirements` (`cur_rowless_blobloss`). `fix_floorfirst` keeps `INV_COMMITTED_BLOBS_PRESENT`
  green, so A'' closes both losses under the same assumption.
- A timed-out blob or staged-manifest PUT does not pin the build. It is harmless debris unless it can be
  committed, which needs a later `precommitAdd`/promote of the same operation. The model keeps staging
  atomic for that reason.

What the code must document or enforce: the floor may retire an epoch only after every operation admitted
under it has ended. At the base nothing enforces this for a superseded epoch; for a fence-out it holds
only if the request deadline is shorter than the wait before the fence-out is acted on.

## PR 2528 recheck shape {#pr-2528-recheck-shape}

https://github.com/Altinity/ClickHouse/pull/2528 fixes CAS-36 instead of A''. It closes the rowless race
but opens one for a namespace removed and reborn during the page. The life comparison below closes that.

### Shape modeled {#pr-2528-shape-modeled}

`planManifestCursorPage` per page: LIST → catalog cut C → floor F for every key (`floorForNamespace` +
`prefixEligibleUnder`, no `catalog_entry` guard) → owner view for namespaces with a row at C → one
re-read C' of `CasRefCatalog` after all floor reads, for candidates rowless at C; such a candidate is
retained if its row appears in C'.

| Constant | Meaning |
| --- | --- |
| `FixRecheckAfterFloor` | the sweep order above, with `GSwRecheck` (C') as its last step; set together with `FixFloorAll` |
| `ViewByCutLife` | the owner view replays the life C named (`NamespaceLifeId::fromCatalogEntry(catalog_entry)` → `readCkpt` → `activeManifestKeys`); an owner transition journaled under a later life is not in it. New variable `ownLife[m]`: the life `precommitAdd` journaled m under |
| `PromoteSkipsOwnBodyReread` | `PartWriteTxn::promote` takes blob dependencies from `staged_manifests` and skips the body GET; blob checks stay |
| `RecheckLifeChange` | candidate fix: C' applies to every candidate, and one whose life differs between C and C' is retained |

### Results {#pr-2528-results}

TLC `-workers 4`, `-Xmx8g`, `nice 10`. Base constants of `fix_floorfirst` (removal on, `MaxLife = 2`)
unless the name says `norebirth`.

| Config | Setting | Outcome | Generated / distinct | Depth or trace | Wall |
| --- | --- | --- | ---: | ---: | ---: |
| `fix_recheck` | PR shape, life-scoped view | `INV_NO_DANGLE` violated | 1,406,392 / 402,748 | 16 states | 6 s |
| `fix_recheck_norebirth` | PR shape, no removal | green | 34,576,075 / 6,916,256 | 49 | 84 s |
| `fix_recheck_noreread_norebirth` | + promote skips its body GET | green | 34,613,894 / 6,916,256 | 49 | 104 s |
| `fix_recheck_life` | + promote skip + `RecheckLifeChange`, removal on | green | 349,572,301 / 64,960,073 | 52 | 923 s |
| `sab_recheck` | PR shape without C' | `INV_NO_DANGLE` violated | 209,813 / 63,439 | 13 states | 2 s |
| `fenced_late_bound_recheck` | PR shape + `GFence`, `AdmissionBound` | green | 54,044,050 / 9,622,776 | 45 | 118 s |
| `fenced_late_unbounded_recheck` | same, no `AdmissionBound` | `INV_NO_DANGLE` violated | 973,529 / 256,492 | 15 states | 3 s |
| `fix_floorfirst_cutlife` | A'' with the life-scoped view | green | 175,217,318 / 32,507,589 | 50 | 407 s |
| `fix_floorfirst` | A'' (rerun) | green | 199,361,194 / 36,834,717 | 50 | 473 s |

Every green row also checks `INV_COMMITTED_BLOBS_PRESENT`. `fix_floorfirst`'s distinct count grew
(23,771,233 → 36,834,717) because `ownLife` and `swCutLife` are now tracked under `EnableCatalogRows`. Legacy
rows are unchanged (`empty_namespaces` 696, `stage0` 19,846, `stage1` 402,034). `fix_recheck_life` uses
923 s of the 1200 s limit.

### Findings {#pr-2528-findings}

- **The rowless race is closed** (`fix_recheck_norebirth`). If F says a build ended, its `precommitAdd`
  happened before F and needed a row, so C' (after F) shows it. Removing C' reopens the race (`sab_recheck`).
- **Skipping the body GET in promote changes nothing** (`fix_recheck_noreread_norebirth`, same distinct
  count). Under the PR shape the sweep never deletes a body before its promote.
- **Late requests (CAS-36 residual) behave the same as under A''.** `fenced_late_unbounded_recheck`
  hits a namespace with a row at C, as before: the late `precommitAdd`/promote lands after the owner
  view. `fenced_late_bound_recheck` is green.
- **Ordering subtlety: a removal and rebirth between C and F loses the precommit** (`fix_recheck`):
  1. `WStageManifest(m1)` (`n1` rowless), `WCreateNamespace(n1)` (life 1), `GSwList`, `GSwCatalog`:
     C names life 1.
  2. `WRemoveNamespace(n1)` (no owners yet), `WCreateNamespace(n1)` (life 2).
  3. `WPrecommitAdd(m1)` journals under life 2; `WPromote`; `WEndBuild`.
  4. `GSwFloor` (ended), `GSwView` (replays life 1: no owner), `GSwRecheck` (not applied: `n1` had a row at C).
  5. `GSwNominate`, `GSwDelete(m1)`: committed ref over a missing body.

  A'' does not have this gap (`fix_floorfirst_cutlife` green). The cut comes after F, so it names the
  life that holds every transition of an ended build. `RecheckLifeChange` closes it for the PR shape
  (`fix_recheck_life`): retain any candidate whose namespace life changed between C and C'. Code cost:
  the C' read is taken whenever the page has a candidate, not only a rowless one, and the comparison
  uses `catalog_entry->incarnation`.
- **Open for the code check.** The gap is real only if `activeManifestKeys` for a removed life returns an
  empty owner set instead of failing. It fails closed only if `readCkpt` finds life 1's `_ckpt` absent,
  or `manifestDeletionPremise` refuses. Whether terminal namespace removal
  (`CasRefCatalog::deleteCompletedRemovingAtSnapshot`) deletes the old life's `_ckpt` before the row is
  gone is not modeled. The life pin (`FixPinLife`) would also close this, by refusing a precommit under
  a life other than the staging life.
