# Audit of `docs/superpowers/models/` against the blob deletion intents protocol {#model-audit}

Spec: `docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md` (HEAD of `cas-gc-rebuild`).
New-protocol evidence: `tmp/intents/reports/tla-final.md` and the model of record prepared in
`tmp/intents/models/record/` (to be committed as `CaBlobIntentProtocol*`). Code read: `cas-pr1` at `0aa837e942e`.
"The record model" below means that model. Its own header says: "Destruction suppressed is not modelled: every cut
is complete."

## Verdict {#verdict}

**No existing model's guarantee breaks under the intent protocol.** Two of them would have gone unverified, and
both are now re-checked green with the intent rules in place:

1. **No loss when a round's cut misses recent edges, and when destruction is suppressed.** `CaRetiredInRun`
   checks settlement under a cut that covers only edges journalled before the previous adoption.
   `CaGcAckFloorCore`, `CaGcCondemnMarkerGate` (`SuppressRounds`) and `CaGcDestructiveGateCore` check that
   suppressed rounds do nothing destructive. The record model has neither: its cut is atomic and complete, and no
   round is suppressed. The intent protocol also changes what a suppressed round does. G2 creates the intent and
   commits `pending` there (today a suppressed round only condemns: `closeBlob` has no `suppress_destructive`
   term). G7 deletes an intent there. Re-check `CaRetiredInRunIntent`: green; every sabotage red.
2. **`CaRetiredInRun`'s pacing claim inverts.** Its sabotage `no_pacing` ("fresh condemn minted directly
   `delete_pending`") is red, and the README calls the one-round gap load-bearing. G2 of the intent protocol is
   exactly that shape: `pending(t)` in the condemning round. It is safe because the create is confirmed before the
   commit and W4 reads the store (A2, W7). That closes the stale read that `WriterStaleReuse` models. The same
   re-check is green with `pending` at birth.

Also re-checked: the relink path under two-round pacing (`CaRelinkConfirmCoreIntent`, green, sabotages red).

**What the re-check shows the protocol needs from cuts** (sabotage `no_floor`): the cut of the round that runs G4
or G8 must include every edge journalled before the previous `gc/state` commit. That is weaker than A7 as the spec
words it ("every live or precommit edge is in a cut that authorizes a delete") and is what the protocol uses. An
unsuppressed cut that may miss such an edge loses data. The trace does not depend on intents: the incomplete cut
alone deletes a referenced body.

**Spec gaps found (prose, no defect):**
- §3.2 does not say that G2 and G7 run in suppressed rounds, nor why that is safe. `CaGcDestructiveGateCore`
  states the opposite principle ("Condemnation is an irreversible physical decision in its own right, so it checks
  the gate"). The re-check shows that G2's `pending` and G7's intent delete need no gate. A reader of the gate
  model will expect them gated.
- §3.2 does not say how `standing` and `candidate` rows enter `Gc::graduationDue` (the force-fold trigger of
  `CaGcRoundDeferCore`). If they do not, G8 waits up to `gc_fold_max_defer_rounds`: liveness only, since a deferred
  round makes no decision.
- §10.1 "Required before implementation" does not list a green run with `StagedCopy` and `SourceChanges` on.
  `CaBlobPublishCore`'s publication-integrity properties depend on it. The earlier shape had `safe_integrity`; the
  record model's cfg set has only the two sabotages (`sab_staging_alias_two_copies`, `sab_unchecked_stream`).

## NEEDS REWORK {#needs-rework}

- `CaRetiredInRun`: its `no_pacing` claim is false for the intent protocol, and its writer reads a marker. Rework
  done as `CaRetiredInRunIntent`. Keep the original as the record of the marker settlement, with a header note.

No other module needs rework.

## Classification {#classification}

38 modules (the `CaBlobIntent*` modules excluded). Full greps per module, no truncation, over `.meta`, `meta`,
`marker`, `ondemn`, `raduat`, `Clean`, `onfirm`, `ublish`, `euse|Resurrect|adopt`, `Delete`, `Fold`, `Cut`,
`edelete`, `nDegree`, `elink|ardlink`, `taging|romote`, `sck`, `ebuild`, `token`. Every module with a hit on the
marker terms was then read in context. Most `marker` hits are other markers (mount farewell, B140 expansion marker,
ref-lane cache marker). Most `Clean` hits are `Cleanup`.

Classes:
- **U**: unaffected.
- **A**: abstraction still valid. The protocol must provide the named assumption.
- **S**: stale but harmless. The module describes the marker; the property moved to the record model.
- **R**: needs rework.

| Module | What it models | Class | The marker assumption, and where the spec provides it |
|---|---|---|---|
| `CaGcCondemnMarkerGate` | graduation gated on a confirmed `Condemned` marker; CAS-360 fixes; round stamps | S | "a writer adopts only on a marker that is not `Condemned`; graduation needs a confirmed `Condemned`". Replaced by W4 and the confirmation table of §3.2 (record `sab_attempted_create_confirms`, `sab_absent_conflict_confirms` red). `SuppressRounds` → `CaRetiredInRunIntent`. `TokenRecur` → A1 (record `sab_resend_envelope`). `DeposedDelete` → record's two GC actors with late `bd`. `OldWriter` → `CaBlobIntentMigration` |
| `CaBlobPublishCore` | writer split `HEAD`/marker/publish, envelope identity, late landing, verbatim copy once, resend | S | marker read, reconcile and backfill go (W6). Kept: fresh envelope after an intent (W5), verbatim copy only after an absent W2 and only once (§3.1), payload identity (I2, §6.2), precommit first (W1), resend (A1). Record: `sab_staging_alias_two_copies`, `sab_unchecked_stream`, `sab_resend_envelope`. Missing: the green integrity run (see verdict) |
| `CaEdgeBeforeObserve` | edge before observe, K1 adopt check, ack-floor graduation, tokenless leaf | A | "an adoption after condemnation sees it; a delete needs a pass after that". Spec: W1, W7, I3; G2 confirms before the commit; G4 needs a cut after the `pending` commit (I5). Tokenless leaf: `PartWriteTxn::adoptEvidence` sends no request and never read the marker; §4.2 rests it on the live source edge; record `safe_linker` |
| `CaRetiredInRun` | settlement inside the snapshot run; coverage-floor cut; one-round stale meta read | R | "a meta read may lag one round, so graduation waits one round". The intent protocol mints `pending` at birth and relies on a confirmed create plus a linearizable W4 instead. Re-checked: `CaRetiredInRunIntent` |
| `CaRetiredInRunFoldAbortWitness` | add-only GC meta: a spare never clears `Condemned`; two leaders | S | "a spare leaves the marker". Spec: G6, G11. Record: `sab_spare_deletes_intent` red, `safe_late_body_delete` green. Also `CaRetiredInRunIntent` `sab_spare_deletes_intent` red |
| `CaGcDestructiveGateCore` | physical gate: anomalies, holds, complete frontier | A | "every physical delete checks the gate". New physical delete: G8's intent `DELETE`, gated (§3.2 G8; I4 (a)). G7's intent delete is not gated: I4 (b). G2 commits `pending` in suppressed rounds. Re-checked: `sab_g8_suppressed`, `sab_g4_suppressed` red; base (G7 and G2 in suppressed rounds, witness reached) green |
| `CaGcRoundDeferCore` | a round defers only when no destructive decision is due | A | "no destructive decision on a not-fully-folded snapshot". Spec: every decision is in the fold, after the cut (§3.2 "Order inside one round", G12). Gap: `graduationDue` and the new rows (verdict) |
| `CaGcAckFloorCore` | one-pass round, writer ack floor, clamp suppression, rebuild | A | clamp: "a clamped pass makes no destructive decision" → G3, G4, G8, G9 gated, G2 and G7 not (re-checked). Rebuild: "restart with no retired rows" → §3.4, rows dropped, intents stand, G9 adopts; record `safe_rebuild`, `safe_rebuild_generation`. Ack floor: already superseded |
| `CaGcAckFloorZombie` | two leaders; delete only rows published `pending` by an earlier commit | A | spec: G4 reads `pending`, G8 reads `standing`, both from the adopted state. Record: `sab_delete_on_graduation_cut`, `sab_cut_before_commit` red with two GC actors |
| `CaRelinkConfirmCore` | publish-then-confirm relink; three-phase GC with spare | A | "a blob needs three zero rounds to die". The intent protocol needs two. The relink path sends no blob request, so the intent itself is irrelevant. Re-checked: `CaRelinkConfirmCoreIntent` |
| `CaRefDeltaIntakeCore` | pool-wide fold over ref deltas, holds, two-phase pacing | A | `WAppendDurable` resets `condemned`: "an add durable after condemnation rematerializes". Spec: W1, then W2, then W4; the create confirmed before the `pending` commit; I4. The model's `Condemn` maps to the create being applied; `rsc >= 1` maps to G4 reading `pending` from the adopted state |
| `CaIncarnationCore` | broad writer/GC incarnation core | S (blob gate), U (rest) | `CondemnedAtView` / `WResurrect` stand for the adopt gate; blob adopt safety now lives in the record model. Trees, registry, journal coverage unaffected |
| `CaGcShardIncarnationCore` | registry removal: shard incarnation, newborn floor | S (blob gate), U (rest) | as above |
| `CaGcRootLocalPartManifestCore` | part-manifest GC, orphan sweep, attempt scoping | S (blob gate), U (rest) | `BlobCondemnedInView` commit gate is fence-era; the intent protocol has no commit-time blob recheck |
| `CaBuildRootPrecommit` | precommit-first reachability, fail-closed commit | U | GC deletes atomically at in-degree 0; no marker |
| `CaRefLogOnlyAppendCore` | log-only append; GC delete collapsed to one step | U | — |
| `CaRefFoldClampRecoveryCore` | clamped fold recovery; orphan-manifest nomination | U | G9 copies the nomination pattern; no shared state |
| `CaRelinkLaneComposition` | relink seam over the ref lane | U | — |
| `CaB140DangleMerge` | historical journal-trim dangle | U | "marker" is the expansion marker |
| `CaErasureProof` | historical natural erasure | U | `_pool_meta` as identity sentinel only |
| `CaDiskLifecycle` | FORGET lifecycle | U | "marker" is the farewell |
| `CaEpochAllocCore` | writer epochs, decommission, `_pool_meta` hwm | U | §2.2: every `_pool_meta` read-modify-write keeps `token_dialect`; an older build is refused at floor 4 |
| `CaMountObserveCore` | observer timing of the mount fence | U | the tool's M2 reuses this rule with its poll step as cadence; M2 and M7 are not modelled (§10.1) |
| `CaCasMountCore`, `CaCasMountCoreLateRenew`, `CaMountRenewRetryCore`, `CaGcLeaseCore` | mount and GC lease | U | M6's header-4 `gc/state` stops an older GC at its lease read (§2.3) |
| `CaRefTableSnapshotLogCore`, `CaRefCatalogCore`, `CaRefLaneCore`, `CaRefPreFoldDrainCore`, `CaRefPreFoldDrainAllRowsCore`, `CaRefNsCleanupStaleLeaderCore`, `CaRefCleanupLicenceCore`, `CaNamespaceJanitorCursorCore`, `CaRefWriterCleanupCore`, `CaWriterEpochBackfillCore` | ref chain, catalog, lanes, janitors, cleanup | U | ref side only |
| `TlcTemporalSmoke` | TLC jar smoke | U | — |

Topics asked about, not owned by any existing model: `cas-fsck` (only the intent models), staging promotion (no
blob marker on that path), GC-state rebuild (`CaGcAckFloorCore`, record `safe_rebuild*`).

## Re-checks {#rechecks}

Runs: TLC from `tmp/tla2tools.jar`, `-workers 6`, under `flock tmp/intents/models/run.lock`, `timeout 600`, in
`tmp/intents/models/audit/`. Logs: `tmp/intents/models/audit/logs/` (`summary.tsv` is the last pass over the final
files; `summary_batch12.tsv` the first passes and the large runs). Metadirs removed. A red row's state count is
where a worker hit the violation and varies by a few percent between runs; green counts are exact.

### `CaRetiredInRunIntent` {#rir-intent}

`CaRetiredInRun` with the marker replaced by §3.1 and §3.2:
- The writer is W1 (edge), W2, W4, W5 as separate steps.
- GC rows are `cand`, `pend`, `stand`, applied in the spec's order: G2 before G9, G7 before G4 and G8.
- The cut keeps `CaRetiredInRun`'s coverage floor (it may miss edges journalled after the previous adoption).
- A suppressed round may cut anywhere at or after the adopted cut. G3, G4, G8 and G9 do not run in it; G2, G6 and G7
  do.
- Dropped: attempt keys (owned by `CaRetiredInRun`), late requests, unconfirmed creates and a second GC actor (owned
  by the record model). A round is one atomic step.

| cfg | Bounds | Expected | Result | Distinct states |
|---|---|---|---|---|
| `base` | 1 blob, 6 rounds, journal 6, 3 tokens | green | green | 8,119 |
| `base_large` | 1 blob, 9 rounds, journal 9, 4 tokens | green | green | 157,101 |
| `base_2b` | 2 blobs, 5, 5, 3 | green | green | 91,498 |
| `base_2b_large` | 2 blobs, 7, 7, 4 | green | green | 3,050,524 |
| `g7_gated`, `g7_gated_large` | G7 also waits for an unsuppressed round | green | green | 7,660; 127,686 |
| `sab_g8_suppressed` | G8 in a suppressed round | `INV_NO_LOSS` | red | 1,436 |
| `sab_g4_suppressed` | G4 in a suppressed round | `INV_NO_LOSS` | red | 259 |
| `sab_g8_positive` | S3: G8 at positive in-degree | `INV_NO_LOSS` | red | 1,289 |
| `sab_spare_deletes_intent` | S4 | `INV_NO_LOSS` | red | 1,370 |
| `sab_delete_on_condemn_cut` | S5: G2 also deletes the body | `INV_NO_LOSS` | red | 161 |
| `sab_no_floor` | an unsuppressed cut may miss edges before the previous commit | `INV_NO_LOSS` | red | 226 |
| `witness_adopt`, `_g4`, `_g8`, `_g7`, `_g7_suppressed`, `_g9` | reachability | reached | reached | 203 to 1,804 |

Reduced trace of `sab_g8_suppressed`:
1. A writer publishes token 1, then withdraws its edge.
2. Round 1 cuts after the withdrawal: zero, touched. G2 creates `I(B,1)` and commits `pend(1)`.
3. A writer adds its edge (after round 1's commit) and its W2 shows 1.
4. Round 2, unsuppressed, cuts at the floor, which excludes that edge. G4 deletes body 1 and commits `stand(1)`.
   This is allowed: the edge came after the `pending` commit, so the writer's W4 must meet the intent.
5. Round 3 is suppressed and cuts below the floor again. The sabotage lets G8 delete `I(B,1)`.
6. The writer's W4 answers 404. It adopts a deleted body.

In the honest model, step 5 waits for an unsuppressed round. That round's cut includes the edge, and G6 spares.

### `CaRelinkConfirmCoreIntent` {#relink-intent}

`CaRelinkConfirmCore` with one change in `GSettle`: a newly zero blob enters `condemned` and `pendingDelete` in the
same round, and dies in a later round whose fold still shows zero.

| cfg | Expected | Result | Distinct states |
|---|---|---|---|
| `main` | green | green | 260,654 |
| `main2r` (2 receivers, `MaxId = 7`, `MaxRound = 6`) | green | green, 390 s | 84,259,004 (the original: 38,972,632) |
| `sab_publishafterconfirm` | `PromotedNeverDangles` | red | 32,011 |
| `sab_nogate1` | `ConfirmedRelinkNeverDangles` | red | 19,410 |
| `sab_nofence` | `ConfirmedRelinkNeverDangles` | red | 36,418 |
| `witness_delete` | reached | reached | 1,007 |

`_main` keeps `MaxHoles = 0`, so it inherits the original's condition on `LIST` completeness.

## Recommended actions, by importance {#actions}

1. **Commit `CaRetiredInRunIntent`** (done in the working tree, not committed) and add a header note to
   `CaRetiredInRun`: its `no_pacing` sabotage is a marker-era property; the intent protocol mints `pending` at birth
   and relies on a confirmed create plus W4 (A2, W7).
2. **Spec prose:** in §3.2, say that G2 and G7 run in suppressed rounds and why (an intent and a `pending` row
   delete nothing; G7 deletes only after observing the replacement). Point to the re-check. Restate A7 as the
   property the protocol uses: the cut of a round that runs G4 or G8 includes every edge journalled before the
   previous `gc/state` commit.
3. **Spec prose:** in §3.2 or §9, say how `standing` and `candidate` rows enter `Gc::graduationDue`.
4. **Record model:** add a green integrity run (`StagedCopy` and `SourceChanges` on, both dialects). It is the only
   kept `CaBlobPublishCore` property without a green run on the current shape.
5. **Retire** `CaGcCondemnMarkerGate` and `CaRetiredInRunFoldAbortWitness` when the code drops the marker, moving
   them to the README's "Removed models" table with the reason: the property is the record model's confirmation
   table and G6/G11.
6. **Header notes**:
   - `CaBlobPublishCore`: retire after action 4 lands.
   - `CaEdgeBeforeObserve`: K1 is now W4; graduation is "a cut after the `pending` commit".
   - `CaGcDestructiveGateCore`: G8 is a new physical delete under the gate; G2 and G7 run in suppressed rounds.
   - `CaGcAckFloorCore`, `CaGcAckFloorZombie`: clamp and two-phase delete now mean G4 and G8.
   - `CaGcRoundDeferCore`: the force-fold term must cover G4 and G8 rows.
   - `CaRefDeltaIntakeCore`: `Condemn` maps to the applied intent create.
   - `CaRelinkConfirmCore`: two-round pacing is re-checked in the `Intent` variant.
   - `CaIncarnationCore`, `CaGcShardIncarnationCore`, `CaGcRootLocalPartManifestCore`: the blob "condemned in
     view" gate is not the shipped mechanism.
7. **README**: its audit note names `CaGcCondemnMarkerGate`, `CaGcAckFloorZombie` and `CaRetiredInRun` as the
   current gates of GC round safety. Replace them with the record model and the two `Intent` variants.

## Not done {#not-done}

- No liveness check in either variant. Liveness of the intent rules is the record model's (`tla-final.md`).
- `CaRetiredInRunIntent` has one writer per blob and atomic GC rounds. Two writers on one key, late requests and a
  deposed GC actor are the record model's; their combination with suppression and coverage-floor cuts was not run.
- G9 picks the orphan by `CHOOSE`, so the variant adopts one fixed orphan per key, not any.
- No variant of `CaGcAckFloorCore`, `CaGcAckFloorZombie`, `CaRefDeltaIntakeCore` or `CaEdgeBeforeObserve`. Their
  assumptions were checked by reading the spec and the record model's runs only.
- The original module suites (`run_*.sh`) were not re-run; nothing in them changed.

## Files added {#files}

In `docs/superpowers/models/` (not committed):
- `CaRetiredInRunIntent.tla` and 18 cfgs `CaRetiredInRunIntent_*.cfg`.
- `CaRelinkConfirmCoreIntent.tla` and 6 cfgs `CaRelinkConfirmCoreIntent_*.cfg`.

Scratch: `tmp/intents/models/audit/` (the same files, `run_batch.sh`, `run_audit.sh`, `batch*.txt`, `logs/`).

`docs/superpowers/models/README.md` shows as modified in the working tree; this audit did not touch it.
